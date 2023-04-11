
# clean the environment
rm(list=ls())

# config/options
do_ingest_xml <- FALSE
do_save_csvs <- TRUE

# simple logging
logf <- function(s, ...) {
  print(Sys.time())
  cat(sprintf(paste(s, "\n", sep=""), ...))
}

# install dependencies
deps <- c('XML', 'RSQLite', 'httr')
missingDeps <- setdiff(deps, installed.packages()[,'Package'])
if (length(missingDeps)) {
  install.packages(missingDeps)
  logf('Installed %s missing packages', length(missingDeps))
}

library(XML)
library(RSQLite)
library(httr)

# confirm dependencies
print(sprintf('R: %s', R.version.string))
for (dep in deps) {
  print(sprintf('%s: %s', dep, packageVersion(dep)))
}

# establish constants
xmlFile <- 'pubmed.xml'
dtdFile <- 'pubmed.dtd'
dbFile <- 'pubmed.db'

# download the XML and DTD
xmlUrl <- 'https://raw.githubusercontent.com/jhautala/CS5200-prax2/main/pubmed.xml'
httr::GET(url=xmlUrl, httr::write_disk(xmlFile))
dtdUrl <- 'https://raw.githubusercontent.com/jhautala/CS5200-prax2/main/pubmed.dtd'
httr::GET(url=dtdUrl, httr::write_disk(dtdFile))
logf('Downloaded XML and DTD')

# read XML with validation
xmlObj <- xmlParse(xmlFile, validate=TRUE)
logf('Parsed XML with validation')

# --- XML extraction utilities
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

# TODO: delete this?
extract_date <- function(node, xpath) {
  value <- extract_value(node, xpath)
  if (is.na(value)) {
    value <- NA
  } else {
    value <- as.Date(value)
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
      pub_month <- match(extract_value(pubdate_node, 'Month'), month.abb)
      pub_day <- extract_integer(pubdate_node, 'Day')
      pub_season <- extract_value(pubdate_node, 'Season')
      medline_date <- extract_value(pubdate_node, 'MedlineDate')
      # TODO: add cases to extract what we can from MedlineDate?
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
  if (!is.null(validYN)) {
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
    
    # initialize primary keys
    journal_id <- NA
    
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
      journal_id <- NA
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
        author_list_complete <- 'Y' == author_list_complete_YN
      }
      
      author_nodes <- xmlChildren(author_list_node)
      for (author_node in author_nodes) {
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
    
    articles[[pmid]] = c(
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
  logf('Finished ingestion')
  
  # --- Conversion to data frames
  # create author dataframe
  authors <- as.list(authors)
  author_cols <- c(
    "author_id",
    "last_name",
    "first_name",
    "initials",
    "suffix",
    "collective_name",
    "affiliation"
  )
  author_mat <- matrix(
    NA,
    nrow=length(authors),
    ncol=length(author_cols),
    dimnames=list(NULL, author_cols)
  )
  for (i in seq_along(authors)) {
    author <- authors[[i]]
    author_mat[i, 'author_id'] <- i
    author_mat[i, 'last_name'] <- author@last_name
    author_mat[i, 'first_name'] <- author@first_name
    author_mat[i, 'initials'] <- author@initials
    author_mat[i, 'suffix'] <- author@suffix
    author_mat[i, 'collective_name'] <- author@collective_name
    author_mat[i, 'affiliation'] <- author@affiliation
  }
  author_df <- data.frame(author_mat, stringsAsFactors=FALSE)
  logf('Created author data frame (nrow=%s; ncol=%s)', nrow(author_df), ncol(author_df))
  
  # create journal dataframe
  journals <- as.list(journals)
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
  logf('Created journal data frame (nrow=%s; ncol=%s)', nrow(journal_df), ncol(journal_df))
  
  # create article dataframe
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
  article_df$pmid <- row.names(article_df)
  row.names(article_df) <- NULL
  cols <- colnames(article_df)
  article_df <- article_df[, c(cols[length(cols)], cols[1:length(cols)-1])]
  logf('Created article data frame (nrow=%s; ncol=%s)', nrow(article_df), ncol(article_df))
  
  # create join table for authors to articles
  article_author_df <- do.call(rbind, lapply(article_authors, function(aa) {
    data.frame(
      pmid=aa['pmid'],
      author_id=aa['author_id'],
      is_valid=aa['is_valid'],
      stringsAsFactors=FALSE,
      row.names=NULL
    )
  }))
  logf(
    'Created article_author data frame (nrow=%s; ncol=%s)',
    nrow(article_author_df),
    ncol(article_author_df)
  )
  
  # --- Save output
  if (do_save_csvs) {
    # save results of ingestion to working directory
    # NOTE: if we want to make these files 'canonical', copy them to the data directory
    write.csv(journal_df, 'journal_df.csv')
    write.csv(author_df, 'author_df.csv')
    write.csv(article_df, 'article_df.csv')
    write.csv(article_author_df, 'article_author_df.csv')
    logf('Finished writing to CSV')
  } else {
    log('Skipped saving CSVs')
  }
} else {
  # load extant 'canonical' CSVs from data directory
  journal_df <- read.csv('data/journal_df.csv')
  author_df <- read.csv('data/author_df.csv')
  article_df <- read.csv('data/article_df.csv')
  article_author_df <- read.csv('data/article_author_df.csv')
  logf('Loaded extant CSVs')
}

# --- Create schema

