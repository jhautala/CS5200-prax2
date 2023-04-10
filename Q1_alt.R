
# clean the environment
rm(list=ls())

# install dependencies
deps <- c('xml2', 'RSQLite', 'httr', 'purrr', 'parallel')
missingDeps <- setdiff(deps, installed.packages()[,'Package'])
if (length(missingDeps)) {
  install.packages(missingDeps)
}

library(xml2)
library(RSQLite)
library(httr)
library(purrr)
library(parallel)

# confirm dependencies
print(sprintf('R: %s', R.version.string))
for (dep in deps) {
  print(sprintf('%s: %s', dep, packageVersion(dep)))
}

# establish constants
dbName <- 'pubmed.db'
xmlFile <- 'pubmed.xml'
dtdFile <- 'pubmed.dtd'

# download the XML and DTD
xmlUrl <- 'https://raw.githubusercontent.com/jhautala/CS5200-prax2/main/pubmed.xml'
httr::GET(url=xmlUrl, httr::write_disk(xmlFile))
dtdUrl <- 'https://raw.githubusercontent.com/jhautala/CS5200-prax2/main/pubmed.dtd'
httr::GET(url=dtdUrl, httr::write_disk(dtdFile))

# read the XML with validation
doc <- read_xml(xmlFile, validate=dtdFile)

# alternate method, using the DOCTYPE reference from the XML itself
options(XML_DTD="PARSE")
doc <- read_xml(xmlFile)

# test smaller sample
sampleFile <- 'pubmed_sample.xml'
doc <- read_xml(sampleFile, validate=dtdFile)

# --- XML extraction utilities
extract_string <- function(node, xpath) {
  value <- xml_find_first(node, xpath)
  if (is.na(value)) {
    value <- NA_character_
  } else {
    value <- xml_text(value)
  }
}

extract_numeric <- function(node, xpath) {
  value <- xml_find_first(node, xpath)
  if (is.na(value)) {
    value <- NA_real_
  } else {
    value <- as.numeric(xml_text(value))
  }
  return(value)
}

extract_integer <- function(node, xpath) {
  value <- xml_find_first(node, xpath)
  if (is.na(value)) {
    value <- NA_integer_
  } else {
    value <- as.integer(xml_text(value))
  }
  return(value)
}

# a generic function for getting a unique identifier string from an XML DTO model
setGeneric('xml_dto_id', function(object) {
  standardGeneric('xml_dto_id')
})

# a class to represent Journal XML elements
setClass(
  'JournalXmlDto',
  slots=list(
    issn='character',
    issn_type='character',
    cited_medium='character',
    title='character',
    iso_abbrev='character',
    volume='integer',
    issue='integer',
    pub_year='integer',
    pub_month='integer',
    pub_day='integer',
    pub_season='character',
    medline_date='character'
  )
)

setMethod(
  'xml_dto_id',
  'JournalXmlDto',
  function(object) {
    object@issn
  }
)

#' Extract a Journal DTO from an XML node
#'
#' @description
#' `JournalXmlDto` serves as a constructor for a Journal XML DTO
#' 
#' @param journal_node The XML node to parse
#' 
#' @returns an instance of `JournalXmlDto` with as many fields set as we can extract from `journal_node`
JournalXmlDto <- function(journal_node) {
  # null object pattern
  # TODO: delete this? In practice we never need it...
  if (is.na(journal_node)) {
    return(new('JournalXmlDto'))
  }
  
  # initialize all members
  issn <- NA_character_
  issn_type <- NA_character_
  cited_medium <- NA_character_
  title <- NA_character_
  iso_abbrev <- NA_character_
  volume <- NA_integer_
  issue <- NA_integer_
  pub_year <- NA_integer_
  pub_month <- NA_integer_
  pub_day <- NA_integer_
  pub_season <- NA_character_
  medline_date <- NA_character_
  
  # extract children
  title <- extract_string(journal_node, 'Title')
  iso_abbrev <- extract_string(journal_node, 'ISOAbbreviation')
  
  # extract ISSN subnode
  issn_node <- xml_find_first(journal_node, 'ISSN')
  if (!is.na(issn_node)) {
    issn <- xml_text(issn_node)
    issn_type <- xml_attr(issn_node, 'IssnType')
  }
  
  # extract JournalIssue subnode
  journal_issue_node <- xml_find_first(journal_node, 'JournalIssue')
  if (!is.na(journal_issue_node)) {
    cited_medium <- xml_attr(journal_issue_node, 'CitedMedium')
    volume <- extract_integer(journal_issue_node, 'Volume')
    issue <- extract_integer(journal_issue_node, 'Issue')
    
    # extract PubDate (Year, Month, Day, Season, MedlineDate)
    pubdate_node <- xml_find_first(journal_issue_node, 'PubDate')
    if (!is.na(pubdate_node)) {
      pub_year <- extract_integer(pubdate_node, 'Year')
      pub_month <- match(extract_string(pubdate_node, 'Month'), month.abb)
      pub_day <- extract_integer(pubdate_node, 'Day')
      pub_season <- extract_string(pubdate_node, 'Season')
      medline_date <- extract_string(pubdate_node, 'MedlineDate')
      # TODO: add cases to extract what we can from MedlineDate?
    }
  }
  
  journal <- new(
    'JournalXmlDto',
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
  'xml_dto_id',
  'AuthorXmlDto',
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
    
    return(paste(elems, collapse = '/'))
  }
)

# a convenience function for extracting an Author XML node to DTO
AuthorXmlDto <- function(author_node) {
  # null object pattern
  # TODO: delete this? In practice we never need it...
  if (is.na(author_node)) {
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
  validYN <- xml_attr(author_node, 'ValidYN')
  if (!is.na(validYN)) {
    is_valid <- 'Y' == validYN
  }
  
  # extract children
  last_name <- extract_string(author_node, 'LastName')
  first_name <- extract_string(author_node, 'ForeName')
  initials <- extract_string(author_node, 'Initials')
  suffix <- extract_string(author_node, 'Suffix')
  collective_name <- extract_string(author_node, 'CollectiveName')
  
  # extract ISSN subnode
  affiliation_node <- xml_find_first(author_node, 'AffiliationInfo')
  if (!is.na(affiliation_node)) {
    affiliation <- extract_string(affiliation_node, 'Affiliation')
  }
  
  author <- new(
    'AuthorXmlDto',
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

a1 <- xml_find_first(doc, '/Publications/Article/PubDetails/ArticleTitle')
a1 <- xml_attr(doc, '/Publications/Article@PMID')
a1 <- xml_find_first(doc, '/Publications/Article[@PMID="0"]')
is.na(a1)
xml_text(a1)
xml_attr(a1, 'PMID')



# --- loop-based approach
authors <- list()
journals <- list()
articles <- list()
article_authors <- list()

pmids <- xml_find_all(doc, "/Publications/Article") %>% map(function(x) xml_attr(x, 'PMID'))
for (pmid in pmids) {
  details_node <- xml_find_first(
    doc,
    sprintf('/Publications/Article[@PMID="%s"]/PubDetails', pmid)
  )
  if (is.na(details_node)) {
    message(sprintf('Missing "PubDetails" for PMID="%s"', pmid))
    next
  }
  
  # extract Journal metadata
  journal_node <- xml_find_first(details_node, 'Journal')
  if (is.na(journal_node)) {
    # NOTE: we require that articles are 'in journals'
    message(sprintf('Missing "Journal" for PMID="%s"', pmid))
    next
  }
  
  journal_xml_dto <- JournalXmlDto(journal_node)
  
  # register with our journal vector as needed
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
  
  # extract author list
  author_list_node <- xml_find_first(details_node, 'AuthorList')
  if (is.na(author_list_node)) {
    author_list_complete <- NA
    message(sprintf('Missing "AuthorList" for PMID="%s"', pmid))
  } else {
    author_list_complete_YN <- xml_attr(author_list_node, 'CompleteYN')
    if (!is.na(author_list_complete_YN)) {
      author_list_complete <- 'Y' == author_list_complete_YN
    }
    
    author_nodes <- xml_children(author_list_node)
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
  
  article_title <- extract_string(details_node, 'ArticleTitle')
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


# --- simpler loop
authors <- list()
journals <- list()
articles <- list()
article_authors <- list()
for (article_node in xml_children(xml_root(doc))) {
  pmid <- xml_attr(article_node, 'PMID')
  
  details_node <- xml_find_first(article_node, 'PubDetails')
  if (is.na(details_node)) {
    message(sprintf('Missing "PubDetails" for PMID="%s"', pmid))
    next
  }
  
  # extract Journal metadata
  journal_node <- xml_find_first(details_node, 'Journal')
  if (is.na(journal_node)) {
    # NOTE: we require that articles are 'in journals'
    message(sprintf('Missing "Journal" for PMID="%s"', pmid))
    next
  }
  
  journal_xml_dto <- JournalXmlDto(journal_node)
  
  # register with our journal vector as needed
  if (exists(journal_xml_dto@issn, where=journals)) {
    # TODO: check for mismatched metadata?
  } else {
    # NOTE: we use a simple vector to represent JournalDbDto
    journals[[journal_xml_dto@issn]] <- c(
      issn_type=journal_xml_dto@issn_type,
      title=journal_xml_dto@title,
      iso_abbrev=journal_xml_dto@iso_abbrev
    )
  }
  
  # extract author list
  author_list_node <- xml_find_first(details_node, 'AuthorList')
  if (is.na(author_list_node)) {
    author_list_complete <- NA
    message(sprintf('Missing "AuthorList" for PMID="%s"', pmid))
  } else {
    author_list_complete_YN <- xml_attr(author_list_node, 'CompleteYN')
    if (!is.na(author_list_complete_YN)) {
      author_list_complete <- 'Y' == author_list_complete_YN
    }
    
    author_nodes <- xml_children(author_list_node)
    for (author_node in author_nodes) {
      author_xml_dto <- AuthorXmlDto(author_node)
      author_uid <- xml_dto_id(author_xml_dto)
      if (!exists(author_uid, where=authors)) {
        # NOTE: we reuse the XML DTO as DB DTO
        authors[[author_uid]] <- author_xml_dto
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
  
  # extract journal ID for joining
  journal_id <- which(names(journals) == journal_xml_dto@issn)
  
  # extract children of PubDetails
  article_title <- extract_string(details_node, 'ArticleTitle')
  
  # convert to vector and save to our list of articles
  articles[[pmid]] <- c(
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

# --- parallel solution WIP
num_cores <- detectCores()

# determine matrix sizes
xml_find_all(doc, "/Publications/Article")

lock <- makeCluster(1)

authors <- list()
journals <- list()
articles <- list()
article_authors <- list()

process_article <- function(article_node) {
  pmid <- xml_attr(article_node, 'PMID')
  
  details_node <- xml_find_first(article_node, 'PubDetails')
  if (is.na(details_node)) {
    message(sprintf('Missing "PubDetails" for PMID="%s"', pmid))
    next
  }
  
  # extract Journal metadata
  journal_node <- xml_find_first(details_node, 'Journal')
  if (is.na(journal_node)) {
    # NOTE: we require that articles are 'in journals'
    message(sprintf('Missing "Journal" for PMID="%s"', pmid))
    next
  }
  
  journal_xml_dto <- JournalXmlDto(journal_node)
  
  # register with our journal vector as needed
  if (exists(journal_xml_dto@issn, where=journals)) {
    # TODO: check for mismatched metadata?
  } else {
    # NOTE: we use a simple vector to represent JournalDbDto
    journals[[journal_xml_dto@issn]] <<- c(
      issn_type=journal_xml_dto@issn_type,
      title=journal_xml_dto@title,
      iso_abbrev=journal_xml_dto@iso_abbrev
    )
  }
  
  # extract author list
  author_list_node <- xml_find_first(details_node, 'AuthorList')
  if (is.na(author_list_node)) {
    author_list_complete <- NA
    message(sprintf('Missing "AuthorList" for PMID="%s"', pmid))
  } else {
    author_list_complete_YN <- xml_attr(author_list_node, 'CompleteYN')
    if (!is.na(author_list_complete_YN)) {
      author_list_complete <- 'Y' == author_list_complete_YN
    }
    
    author_nodes <- xml_children(author_list_node)
    for (author_node in author_nodes) {
      author_xml_dto <- AuthorXmlDto(author_node)
      author_uid <- xml_dto_id(author_xml_dto)
      if (!exists(author_uid, where=authors)) {
        # NOTE: we reuse the XML DTO as DB DTO
        authors[[author_uid]] <<- author_xml_dto
      }
      author_id <- which(names(authors) == author_uid)
      article_authors <<- append(
        article_authors,
        list(c(
          pmid=pmid,
          author_id=author_id,
          is_valid=author_xml_dto@is_valid
        ))
      )
    }
  }
  
  # extract journal ID for joining
  journal_id <- which(names(journals) == journal_xml_dto@issn)
  
  # extract children of PubDetails
  article_title <- extract_string(details_node, 'ArticleTitle')
  
  # convert to vector and save to our list of articles
  articles[[pmid]] <<- c(
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
articles_list <- xml_find_all(doc, "/Publications/Article")
processed_articles <- mclapply(articles_list, process_article, mc.cores=num_cores)




# create author dataframe
author_cols <- c(
  'last_name',
  'first_name',
  'initials',
  'suffix',
  'collective_name',
  'affiliation'
)
author_mat <- matrix(
  NA,
  nrow=length(authors),
  ncol=length(author_cols),
  dimnames=list(NULL, author_cols)
)
for (i in seq_along(authors)) {
  author <- authors[[i]]
  author_mat[i, 'last_name'] <- author@last_name
  author_mat[i, 'first_name'] <- author@first_name
  author_mat[i, 'initials'] <- author@initials
  author_mat[i, 'suffix'] <- author@suffix
  author_mat[i, 'collective_name'] <- author@collective_name
  author_mat[i, 'affiliation'] <- author@affiliation
}
author_df <- data.frame(author_mat, stringsAsFactors=FALSE)
print(author_df)

# author_df <- do.call(rbind, lapply(authors, function(author) {
#   list(
#     last_name = author@last_name,
#     first_name = author@first_name,
#     initials = author@initials,
#     suffix = author@suffix,
#     collective_name = author@collective_name,
#     affiliation = author@affiliation
#   )
# }))
# author_df <- data.frame(author_df, stringsAsFactors=FALSE, row.names=NULL)
# str(author_df)
# print(author_df)

# create journal dataframe
journal_df <- do.call(rbind, lapply(journals, function(j) {
  data.frame(
    issn_type=j['issn_type'],
    title=j['title'],
    iso_abbrev=j['iso_abbrev'],
    stringsAsFactors=FALSE
  )
}))
journal_df$issn <- row.names(journal_df)
row.names(journal_df) <- NULL
journal_df <- journal_df[,c('issn', 'issn_type', 'title', 'iso_abbrev')]
print(journal_df)

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
print(article_df)

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
print(article_author_df)


save(journal_df, file='journal_df.Rda')
save(author_df, file='author_df.Rda')
save(article_df, file='article_df.Rda')
save(article_author_df, file='article_author_df.Rda')

write.csv(journal_df, 'journal_df.csv')
write.csv(author_df, 'author_df.csv')
write.csv(article_df, 'article_df.csv')
write.csv(article_author_df, 'article_author_df.csv')

journal_df_tmp <- read.csv('data/draft2/journal_df.csv')
author_df_tmp <- read.csv('data/draft2/author_df.csv')
article_df_tmp <- read.csv('data/draft2/article_df.csv')
article_author_df_tmp <- read.csv('data/draft2/article_author_df.csv')
