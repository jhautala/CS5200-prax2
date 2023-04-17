# NOTE: This OLTP schema is intended to be as close as possible to our upstream
#       data. There is some cleanup/standardizing here, but the only assumption
#       we intend to make is that articles must have journals. We do _not_
#       assume that all articles must have authors.

# config/options
do_ingest_xml <- TRUE
do_save_csvs <- TRUE
do_http_cache <- TRUE

# simple logging
logf <- function(s, ...) {
  print(Sys.time())
  cat(sprintf(paste(s, "\n", sep=""), ...))
}

# install dependencies
deps <- c('XML', 'RSQLite', 'httr', 'stringr')
missingDeps <- setdiff(deps, installed.packages()[,'Package'])
if (length(missingDeps)) {
  install.packages(missingDeps)
  logf('Installed %s missing packages', length(missingDeps))
}

library(XML)
library(RSQLite)
library(httr)
library(stringr)

# confirm dependencies
# This script was developed with the following versions:
#   * R: R version 4.2.1 (2022-06-23)
#   * XML: 3.99.0.14
#   * RSQLite: 2.3.0
#   * httr: 1.4.5
print(sprintf('R: %s', R.version.string))
for (dep in deps) {
  print(sprintf('%s: %s', dep, packageVersion(dep)))
}

# establish constants
xmlFile <- 'pubmed.xml'
dtdFile <- 'pubmed.dtd'
dbFile <- 'pubmed.db'
schemaFile <- 'oltp_schema.sql'

# --- Part 1.5: Realize the schema
# download the OLTP schema
schemaUrl <- 'https://raw.githubusercontent.com/jhautala/CS5200-prax2/main/oltp_schema.sql'
if (do_http_cache) {
  if (file.exists(schemaFile)) {
    logf('Using cached OLTP schema: %s', normalizePath(schemaFile))
  } else {
    httr::GET(url=schemaUrl, httr::write_disk(schemaFile))
    logf('Downloaded OLTP schema')
  }
} else {
  httr::GET(url=schemaUrl, httr::write_disk(schemaFile, overwrite=TRUE))
  logf('Downloaded OLTP schema')
}

# open the database connection and create the schema
db_init <- function() {
  # clear extant DB
  unlink(dbFile)
  
  # create fresh DB
  dbcon <- dbConnect(RSQLite::SQLite(), dbFile)
  
  # enable foreign keys
  dbExecute(dbcon, 'PRAGMA foreign_keys = ON')
  
  # read the schema file
  schema_sql <- readChar(schemaFile, file.info(schemaFile)$size)
  sqls_statements <- strsplit(schema_sql, '\\s*;\\s*')[[1]]
  
  # execute the SQL
  for (sql in sqls_statements) {
    dbExecute(dbcon, paste0(sql, ';'))
  }
  
  return(dbcon)
}
dbcon <- db_init()
on.exit(dbDisconnect(dbcon))

# --- Part 1.6: Load the XML, with validation
# download the XML and DTD
xmlUrl <- 'https://raw.githubusercontent.com/jhautala/CS5200-prax2/main/pubmed.xml'
dtdUrl <- 'https://raw.githubusercontent.com/jhautala/CS5200-prax2/main/pubmed.dtd'
if (do_http_cache) {
  if (file.exists(xmlFile)) {
    logf('Using cached XML: %s', normalizePath(xmlFile))
  } else {
    httr::GET(url=xmlUrl, httr::write_disk(xmlFile))
    logf('Downloaded XML')
  }
  if (file.exists(dtdFile)) {
    logf('Using cached DTD: %s', normalizePath(dtdFile))
  } else {
    httr::GET(url=dtdUrl, httr::write_disk(dtdFile))
    logf('Downloaded DTD')
  }
} else {
  httr::GET(url=xmlUrl, httr::write_disk(xmlFile, overwrite=TRUE))
  httr::GET(url=dtdUrl, httr::write_disk(dtdFile, overwrite=TRUE))
  logf('Downloaded XML and DTD')
}

# read XML with validation
xmlObj <- xmlParse(xmlFile, validate=TRUE)
logf('Parsed XML with validation')

# --- Part 1.7: ETL XML to SQLite
# XML extraction utilities
extract_value <- function(node, xpath) {
  value <- xpathSApply(node, xpath, xmlValue)
  if (length(value) == 0) {
    value <- NA_character_
  }
  return(value)
}

extract_numeric <- function(node, xpath) {
  value <- extract_value(node, xpath)
  if (is.na(value)) {
    value <- NA_real_
  } else {
    value <- as.numeric(value)
  }
  return(value)
}

extract_integer <- function(node, xpath) {
  value <- extract_value(node, xpath)
  if (is.na(value)) {
    value <- NA_integer_
  } else {
    value <- as.integer(value)
  }
  return(value)
}

extract_node <- function(node, xpath, desc=NA) {
  nodes <- xpathSApply(node, xpath)
  if (length(nodes) == 0) {
    if (!is.na(desc)) {
      message(sprintf('Missing %s', desc))
    }
    return(NULL)
  }
  
  if (length(nodes) > 1) {
    if (!is.na(desc)) {
      message(sprintf(
        'Encountered extra %s (found %s, expected 1)',
        desc,
        length(nodes)
      ))
    }
  }
  
  return(nodes[[1]])
}

# establish a regex for parsing months to integers
extract_month <- function(node, xpath) {
  month_string <- extract_value(node, xpath)
  if (is.na(month_string)) {
    return(NA_integer_)
  }
  
  # check for month.abb (expected case)
  month_string <- str_to_title(month_string)
  month_integer <- which(month.abb == month_string)
  if (length(month_integer)) {
    return(month_integer)
  }
  
  # check for integer month (encountered case)
  # NOTE: we cannot simply check for `NA`; the string 'NAN' is
  #       coerced differently from other non-numeric strings...
  month_integer <- suppressWarnings(as.integer(month_string))
  if (!is.na(month_integer) && month_integer > 0 && month_integer < 13) {
    return(month_integer)
  }
  
  # check for month.name (may as well)
  month_integer <- which(month.name == month_string)
  if (length(month_integer)) {
    return(month_integer)
  }
  
  return(NA_integer_)
}

# --- Define DTOs and DTO-specific utility functions
# a generic function for getting a unique identifier string from an XML DTO model
setGeneric(
  "xml_dto_id",
  function(object) {
    standardGeneric("xml_dto_id")
  }
)

# a class to represent Journal XML elements
setClass(
  'JournalXmlDto',
  slots=list(
    issn='character',
    issn_type='character',
    cited_medium='character',
    title='character',
    iso_abbrev='character',
    volume='character',
    issue='character',
    pub_year='integer',
    pub_month='integer',
    pub_day='integer',
    pub_season='character',
    medline_date='character'
  )
)

setMethod(
  "xml_dto_id",
  "JournalXmlDto",
  function(object) {
    object@issn
  }
)

# a function for obtaining a Journal DTO from an XML node
JournalXmlDto <- function(journal_node) {
  if (is.null(journal_node)) {
    return(new('JournalXmlDto'))
  }
  
  # initialize all members
  issn <- NA_character_
  issn_type <- NA_character_
  cited_medium <- NA_character_
  title <- NA_character_
  iso_abbrev <- NA_character_
  volume <- NA_character_
  issue <- NA_character_
  pub_year <- NA_integer_
  pub_month <- NA_integer_
  pub_day <- NA_integer_
  pub_season <- NA_character_
  medline_date <- NA_character_
  
  # extract children
  title <- extract_value(journal_node, "Title")
  iso_abbrev <- extract_value(journal_node, "ISOAbbreviation")
  
  # extract ISSN subnode
  issn_node <- extract_node(journal_node, "ISSN")
  if (!is.null(issn_node)) {
    issn <- xmlValue(issn_node)
    issn_type <- xmlGetAttr(issn_node, "IssnType")
  }
  
  # extract JournalIssue subnode
  journal_issue_node <- extract_node(journal_node, "JournalIssue")
  if (!is.null(journal_issue_node)) {
    cited_medium <- xmlGetAttr(journal_issue_node, "CitedMedium")
    volume <- extract_value(journal_issue_node, 'Volume')
    issue <- extract_value(journal_issue_node, 'Issue')
    
    # extract PubDate (Year, Month, Day, Season, MedlineDate)
    pubdate_node <- extract_node(journal_issue_node, 'PubDate')
    if (!is.null(pubdate_node)) {
      pub_year <- extract_integer(pubdate_node, 'Year')
      pub_month <- extract_month(pubdate_node, 'Month')
      pub_day <- extract_integer(pubdate_node, 'Day')
      pub_season <- extract_value(pubdate_node, 'Season')
      # NOTE: we preserve medline date exactly as it is; we will parse it further during ETL
      medline_date <- extract_value(pubdate_node, 'MedlineDate')
    }
  }
  
  journal <- new(
    "JournalXmlDto",
    issn=issn,
    issn_type=issn_type,
    cited_medium=cited_medium,
    title=title,
    iso_abbrev=iso_abbrev,
    volume=volume,
    issue=issue,
    pub_year=pub_year,
    pub_month=pub_month,
    pub_day=pub_day,
    pub_season=pub_season,
    medline_date=medline_date
  )
  
  return(journal)
}

# a class representing XML data under Author node
setClass(
  'AuthorXmlDto',
  slots=list(
    last_name='character',
    first_name='character',
    initials='character',
    suffix='character',
    collective_name='character',
    affiliation='character',
    is_valid='logical'
  )
)

setMethod(
  "xml_dto_id",
  "AuthorXmlDto",
  function(object) {
    elems <- c()
    
    # iterate over uniqueness constraint
    for (k in slotNames(object)) {
      if ('affiliation' == k || 'is_valid' == k) {
        next
      }
      
      # add the value of the current slot
      v <- slot(object, k)
      if (is.na(v)) {
        elems <- c(elems, '')
      } else {
        elems <- c(elems, v)
      }
    }
    
    return(paste(elems, collapse = "/"))
  }
)

# a convenience function for extracting an Author XML node to DTO
AuthorXmlDto <- function(author_node) {
  if (is.null(author_node)) {
    return(new('AuthorXmlDto'))
  }
  
  # initialize all members
  last_name <- NA_character_
  first_name <- NA_character_
  initials <- NA_character_
  suffix <- NA_character_
  collective_name <- NA_character_
  affiliation <- NA_character_
  is_valid <- NA
  
  # extract attributes
  validYN <- xmlGetAttr(author_node, 'ValidYN')
  if (is.null(validYN)) {
    is_valid <- NA
  } else {
    # NOTE: here we depend on the member type 'logical' to produce [0, 1]
    is_valid <- 'Y' == validYN
  }
  
  # extract children
  last_name <- extract_value(author_node, 'LastName')
  first_name <- extract_value(author_node, 'ForeName')
  initials <- extract_value(author_node, 'Initials')
  suffix <- extract_value(author_node, 'Suffix')
  collective_name <- extract_value(author_node, 'CollectiveName')
  
  # extract ISSN subnode
  affiliation_node <- extract_node(author_node, 'AffiliationInfo')
  if (!is.null(affiliation_node)) {
    affiliation <- extract_value(affiliation_node, 'Affiliation')
  }
  
  author <- new(
    "AuthorXmlDto",
    last_name=last_name,
    first_name=first_name,
    initials=initials,
    suffix=suffix,
    collective_name=collective_name,
    affiliation=affiliation,
    is_valid=is_valid
  )
  
  return(author)
}

# functions for converting lists to data frames
# TODO: Work to make this more consistent and easier to maintain?
#       It would be nice to have the parser map XML DTOs to DB DTOs, then maybe
#       this conversion could be more dynamic and generalized. During initial
#       development, perhaps some things were prematurely optimized (e.g.
#       avoiding "extra" classes and instantiations).
list_to_author_df <- function(authors) {
  author_cols <- c(
    "author_id",
    "last_name",
    "first_name",
    "initials",
    "suffix",
    "collective_name",
    "affiliation"
  )
  author_df <- data.frame(matrix(ncol = length(author_cols), nrow = length(authors)))
  colnames(author_df) <- author_cols
  
  # add the ID column
  author_df$author_id <- seq_along(authors)
  
  # copy the string columns
  i <- 0
  for (author in authors) {
    i <- i + 1
    author_df[i, 'last_name'] <- author@last_name
    author_df[i, 'first_name'] <- author@first_name
    author_df[i, 'initials'] <- author@initials
    author_df[i, 'suffix'] <- author@suffix
    author_df[i, 'collective_name'] <- author@collective_name
    author_df[i, 'affiliation'] <- author@affiliation
  }
  
  # Convert the data frame to strings without factors
  author_df <- data.frame(author_df, stringsAsFactors = FALSE)
  return(author_df)
}

list_to_journal_df <- function(journals) {
  journal_df <- do.call(rbind, lapply(journals, function(j) {
    data.frame(
      issn_type=j['issn_type'],
      title=j['title'],
      iso_abbrev=j['iso_abbrev'],
      stringsAsFactors=FALSE
    )
  }))
  journal_df$issn <- row.names(journal_df)
  row.names(journal_df) <- NULL # reset the index
  journal_df$journal_id <- seq(nrow(journal_df)) # establish journal IDs
  journal_df <- journal_df[,c('journal_id', 'issn', 'issn_type', 'title', 'iso_abbrev')]
  return(journal_df)
}

list_to_article_df <- function(articles) {
  article_df <- do.call(rbind, lapply(articles, function(a) {
    data.frame(
      journal_id=a['journal_id'],
      article_title=a['article_title'],
      cited_medium=a['cited_medium'],
      journal_volume=a['journal_volume'],
      journal_issue=a['journal_issue'],
      pub_year=a['pub_year'],
      pub_month=a['pub_month'],
      pub_day=a['pub_day'],
      pub_season=a['pub_season'],
      medline_date=a['medline_date'],
      author_list_complete=a['author_list_complete'],
      stringsAsFactors=FALSE,
      row.names=NULL
    )
  }))
  
  # assign IDs
  article_df$pmid <- as.integer(names(articles))
  # reset index
  row.names(article_df) <- NULL
  # reorder columns to put primary key first
  cols <- colnames(article_df)
  article_df <- article_df[, c(cols[length(cols)], cols[1:length(cols)-1])]
  
  # correct all the data types that were lost due to due to using a list of vectors
  # NOTE: A list of named lists seems more elegant (especially with dplyr's `bind_rows`),
  #       but I haven't been able to figure out why it produces fewer rows... alas!
  article_df$journal_id <- as.integer(article_df$journal_id)
  article_df$pub_year <- as.integer(article_df$pub_year)
  article_df$pub_month <- as.integer(article_df$pub_month)
  article_df$pub_day <- as.integer(article_df$pub_day)
  article_df$author_list_complete <- as.integer(as.logical(article_df$author_list_complete))
  
  # NOTE: here we must conform to SQLite's implementation of Boolean type
  article_df$author_list_complete <- as.integer(article_df$author_list_complete)
  return(article_df)
}

list_to_article_author_df <- function(article_authors) {
  article_author_df <- do.call(rbind, lapply(article_authors, function(aa) {
    data.frame(
      pmid=aa['pmid'],
      author_id=aa['author_id'],
      is_valid=aa['is_valid'],
      stringsAsFactors=FALSE,
      row.names=NULL
    )
  }))
  return(article_author_df)
}

# --- Ingestion loop
if (do_ingest_xml) {
  # NOTE: we use environments for faster lookup of authors and journals
  authors <- new.env()
  journals <- new.env()
  articles <- list()
  article_authors <- list()
  article_id <- 0
  logf('Starting ingestion')
  for (article_node in xpathSApply(xmlObj, '//Article')) {
    article_id <- article_id + 1
    pmid <- xmlGetAttr(article_node, 'PMID')
    if (is.null(pmid)) {
      message(sprintf('Missing "PMID" for article element %s', article_id))
      next
    }
    pmid <- as.integer(pmid)
    
    details_node <- extract_node(
      article_node,
      'PubDetails',
      sprintf('"PubDetails" for PMID="%s"', pmid)
    )
    if (is.null(details_node)) {
      next
    }
    
    # extract child values
    article_title <- extract_value(details_node, 'ArticleTitle')
    
    # extract Journal metadata
    journal_node <- extract_node(
      details_node,
      'Journal',
      sprintf('"Journal" for article with PMID="%s"', pmid)
    )
    journal_xml_dto <- JournalXmlDto(journal_node)
    if (is.null(journal_xml_dto@issn)) {
      # establish a null reference for journal
      journal_id <- NA_integer_
    } else {
      # register with our journal vector
      if (exists(journal_xml_dto@issn, where=journals)) {
        # TODO: check for mismatched metadata?
      } else {
        # NOTE: we use a simple vector to represent JournalDbDto
        journals[[journal_xml_dto@issn]] = c(
          issn_type=journal_xml_dto@issn_type,
          title=journal_xml_dto@title,
          iso_abbrev=journal_xml_dto@iso_abbrev
        )
      }
      
      # extract journal ID for joining
      # TODO: just use length(journals) for new journal case? probably premature optimization...
      journal_id <- which(names(journals) == journal_xml_dto@issn)
    }
    
    # extract author list
    author_list_node <- extract_node(
      details_node,
      'AuthorList',
      sprintf('"AuthorList" for article with PMID="%s"', pmid)
    )
    
    author_list_complete <- NA
    if (!is.null(author_list_node)) {
      author_list_complete_YN <- xmlGetAttr(author_list_node, 'CompleteYN')
      if (!is.null(author_list_complete_YN)) {
        # NOTE: here we accommodate SQLite data type
        author_list_complete <- 'Y' == author_list_complete_YN
      }
      
      for (author_node in xmlChildren(author_list_node)) {
        author_xml_dto <- AuthorXmlDto(author_node)
        author_uid <- xml_dto_id(author_xml_dto)
        if (!exists(author_uid, where=authors)) {
          # NOTE: we reuse the XML DTO as DB DTO
          authors[[author_uid]] = author_xml_dto
        }
        author_id <- which(names(authors) == author_uid)
        article_authors <- append(
          article_authors,
          list(c(
            pmid=pmid,
            author_id=author_id,
            is_valid=author_xml_dto@is_valid
          ))
        )
      }
    }
    
    articles[[as.character(pmid)]] <- c(
      journal_id=journal_id,
      article_title=article_title,
      cited_medium=journal_xml_dto@cited_medium,
      journal_volume=journal_xml_dto@volume,
      journal_issue=journal_xml_dto@issue,
      pub_year=journal_xml_dto@pub_year,
      pub_month=journal_xml_dto@pub_month,
      pub_day=journal_xml_dto@pub_day,
      pub_season=journal_xml_dto@pub_season,
      medline_date=journal_xml_dto@medline_date,
      author_list_complete=author_list_complete
    )
  }
  logf('Finished extraction')
  
  # --- Conversion to data frames
  author_df <- list_to_author_df(as.list(authors))
  logf('Created author data frame (nrow=%s; ncol=%s)', nrow(author_df), ncol(author_df))
  
  journal_df <- list_to_journal_df(as.list(journals))
  logf('Created journal data frame (nrow=%s; ncol=%s)', nrow(journal_df), ncol(journal_df))
  
  article_df <- list_to_article_df(articles)
  logf('Created article data frame (nrow=%s; ncol=%s)', nrow(article_df), ncol(article_df))
  
  # create join table for authors to articles
  article_author_df <- list_to_article_author_df(article_authors)
  logf(
    'Created article_author data frame (nrow=%s; ncol=%s)',
    nrow(article_author_df),
    ncol(article_author_df)
  )
  
  # --- Save output
  if (do_save_csvs) {
    # save results of ingestion to working directory
    # NOTE: If we want to make these files 'canonical', and use `do_ingest_xml=FALSE`,
    #       we must copy them to the 'data' sub-directory of the working directory.
    # IMPORTANT: The only cleanup we've done in creating these data frames is to ensure
    #            that articles have 'PMID' and journals have 'ISSN'.
    write.csv(journal_df, 'journal_df.csv', row.names=FALSE)
    write.csv(author_df, 'author_df.csv', row.names=FALSE)
    write.csv(article_df, 'article_df.csv', row.names=FALSE)
    write.csv(article_author_df, 'article_author_df.csv', row.names=FALSE)
    logf('Finished writing to CSV')
  } else {
    log('Skipped saving CSVs')
  }
} else {
  # load extant 'canonical' CSVs from 'data' directory
  journal_df <- read.csv('data/journal_df.csv')
  author_df <- read.csv('data/author_df.csv')
  article_df <- read.csv('data/article_df.csv')
  article_author_df <- read.csv('data/article_author_df.csv')
  logf('Loaded extant CSVs')
}

# --- Write data frames to SQLite
# TODO: Deduplicate journals with same title/iso_abbrev?
#       There seem to be some cadndidates, and it seems the `issn_type` doesn't necessarily
#       distinguish distinct journals. Leaving as-is for now, with a bit of code to assess
#       opportunity for deduping.
# NOTE: Such a solution would require updating journal IDs to preclude orphans.
dupe_titles <- journal_df[duplicated(journal_df$title), 'title']
dupe_abbrev <- journal_df[duplicated(journal_df$iso_abbrev), 'iso_abbrev']
dupe_journal_df <- journal_df[(journal_df$title %in% dupe_titles | journal_df$iso_abbrev %in% dupe_abbrev),]
print(dupe_journal_df[order(dupe_journal_df$title),])

# filter out journals missing 'issn'
# NOTE: Since we do not assign journal IDs to journals missing 'ISSN' during ingestion,
#       we do not have to worry about breaking any links by deleting them here.
#       However, as a matter of good practice (i.e. defensive programming), we
#       grab some info here to make sure not to attempt to persist any such articles.
missing_issn_mask <- is.na(journal_df$issn)
missing_issn_df <- journal_df[missing_issn_mask,]

# Check to see if we can identify candidates for missing ISSN by title or iso_abbrev
# TODO: Implement ISSN inference? It would be messy, and apparently not fruitful for the available data
title_match_mask <- journal_df$title %in% missing_issn_df$title
iso_abbrev_match_ask <- journal_df$iso_abbrev %in% missing_issn_df$iso_abbrev
issn_inference_candidate_df <- journal_df[(!missing_issn_mask & (title_match_mask | iso_abbrev_match_ask)),]
n_journals <- nrow(journal_df)
journal_df <- journal_df[!missing_issn_mask,]
logf(
  "Sanitized %s journals -> %s valid journals; deleted %s journals (missing 'ISSN'); %s candidates for inference",
  n_journals,
  nrow(journal_df),
  nrow(missing_issn_df),
  nrow(issn_inference_candidate_df)
)

# cleanup any articles orphaned by journals missing 'ISSN', or otherwise unlinked
n_articles <- nrow(article_df)
article_df <- article_df[article_df$journal_id %in% journal_df$journal_id,]
logf(
  'Sanitized %s articles -> %s valid articles; deleted %s articles missing journal',
  n_articles,
  nrow(article_df),
  n_articles - nrow(article_df)
)

# cleanup any orphaned or redundant article_author records
n_article_authors <- nrow(article_author_df)
redundant_mask <- duplicated(article_author_df[, c("pmid", "author_id")])
missing_author_mask <- !(!is.na(article_author_df$author_id) & article_author_df$author_id %in% author_df$author_id)
missing_article_mask <- !(!is.na(article_author_df$pmid) & article_author_df$pmid %in% article_df$pmid)
valid_mask <- !redundant_mask & !missing_author_mask & !missing_article_mask
article_author_df <- article_author_df[valid_mask,]
logf(
  paste0(
    'Sanitized %s article authors -> %s valid article authors; ',
    'deleted %s records:\n\t',
    '%s redundant\n\t',
    '%s missing authors\n\t',
    '%s missing articles\n\t',
    '%s missing both'
  ),
  n_article_authors,
  nrow(article_author_df),
  n_article_authors - nrow(article_author_df),
  sum(redundant_mask),
  sum(missing_author_mask),
  sum(missing_article_mask),
  sum(missing_author_mask & missing_article_mask)
)

# check relationship between cited_medium and issn_type
merged_df <- merge(article_df, journal_df, by='journal_id')
combos <- unique(paste(merged_df$cited_medium, merged_df$issn_type, sep=':'))
print(paste(combos))

# Execute inserts
tbls <- list(
  author=author_df,
  journal=journal_df,
  article=article_df,
  article_author=article_author_df
)
# # cleanup extant tables
# # NOTE: this is not needed during normal execution, but it is useful when debugging
# for (tbl in rev(names(tbls))) {
#   dbExecute(dbcon, sprintf('delete from %s WHERE 1=1;', tbl))
# }
for (tbl in names(tbls)) {
  df <- tbls[[tbl]]
  dbWriteTable(dbcon, tbl, df, append=TRUE)
  logf("Populated '%s' table (%s rows)", tbl, nrow(df))
}
