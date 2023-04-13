# NOTES:
#   * try to get useful info from MedlineDate?
#   * filter out rows for misspelled authors (i.e. `is_valid=FALSE`)?

do_http_cache <- TRUE

# install dependencies
deps <- c('DBI', 'RSQLite', 'RMySQL', 'stringr', 'tidyverse', 'lubridate')
missingDeps <- setdiff(deps, installed.packages()[,"Package"])
if (length(missingDeps)) {
  install.packages(missingDeps)
}

library(DBI)
library(RSQLite)
library(RMySQL)
library(stringr)
library(tidyverse)
library(lubridate)

# confirm dependencies
# This script was developed with the following versions:
#   * R: R version 4.2.1 (2022-06-23)
#   * DBI: 1.1.3
#   * RSQLite: 2.3.0
#   * RMySQL: 0.10.25
print(sprintf('R: %s', R.version.string))
for (dep in deps) {
  print(sprintf('%s: %s', dep, packageVersion(dep)))
}

# IMPORTANT: Initial DB setup requires at least three things:
#   
#   1. `db_admin_user`: the name of the root user account for your DB server
#   2. `db_admin_pass`: the password for your root user
#   3. `db_owner_pass`: the password to use for the "DB owner" login user account
# 
# It is simplest to create a YAML credentials file named 'db_cred.yml' in the
# working directory, alongside this script.
# 
# NOTE:
#   * Replace asterisks with your names and passwords.
#   * The final line in the file must be empty.
# 
# ### YAML file structure
# ```yaml
# db_admin:
#   name: ************
#   pass: ************
#   db_owner:
#   name: ************
#   pass: ************
# 
# ```
# 
# NOTE: If you don't want to create a YAML credentials config file, you can modify
#       these 'db_' variables (i.e. `db_user` and `db_pass`).

# load credentials from local secrets file
db_cred <- yaml::read_yaml('db_cred.yml')
db_user <- db_cred$db_admin$name
db_pass <- db_cred$db_admin$pass

# establish some constants
db_host <- 'localhost'
db_port <- 3306
olap_schema_file <- 'olap_schema.sql'
olap_schema_url <- 'https://raw.githubusercontent.com/jhautala/CS5200-prax2/main/olap_schema.sql'
if (do_http_cache) {
  if (file.exists(olap_schema_file)) {
    logf('Using cached OLAP schema: %s', normalizePath(olap_schema_file))
  } else {
    httr::GET(url=olap_schema_url, httr::write_disk(olap_schema_file))
    logf('Downloaded OLAP schema')
  }
} else {
  httr::GET(url=olap_schema_url, httr::write_disk(olap_schema_file, overwrite=TRUE))
  logf('Downloaded OLAP schema')
}

# connect to upstream OLTP DB
oltp_dbFile <- 'pubmed.db'
oltp_dbcon <- dbConnect(RSQLite::SQLite(), oltp_dbFile)
on.exit(dbDisconnect(oltp_dbcon))

# connect to the data warehouse
olap_dbcon <- dbConnect(
  MySQL(),
  user=db_user,
  password=db_pass,
  host=db_host,
  port=db_port
)
on.exit(dbDisconnect(olap_dbcon))

# read the schema file
# NOTE: this will clear out and rebuild 'fact' and 'dim' schemas
# TODO: add unique constraint to fact table?
schema_sql <- readChar(olap_schema_file, file.info(olap_schema_file)$size)
sqls_statements <- strsplit(schema_sql, '\\s*;\\s*')[[1]]

# execute the SQL to (re/)create schema
for (sql in sqls_statements) {
  dbExecute(olap_dbcon, paste0(sql, ';'))
}

# Query OLTP DB and iterate over results
sql_query <- (
  'WITH '
)

# some utility functions for parsing MedlineDate
optional_range <- function(s, is_group=FALSE) {
  if (is_group) {
    return(sprintf('(%s)(-(%s))?', s, s))
  } else {
    return(sprintf('(%s)(-%s)?', s, s))
  }
}

year_regex <- regex(optional_range('\\d{4}'))
month_regex <- regex(optional_range(paste(month.abb, collapse='|'), is_group=TRUE), ignore_case=TRUE)
day_regex <- regex('(?<!\\d)(\\d{1,2})(-\\d{1,2})?(?!\\d)')
season_regex <- regex(optional_range('Spring|Summer|Winter|Fall', is_group=TRUE), ignore_case=TRUE)
ymds_regexes <- list(year_regex, month_regex, day_regex, season_regex)

extract_ymds <- function(medline_date) {
  # create a matrix to hold the extracted YMDS
  ymds <- matrix(NA, nrow=length(medline_date), ncol=length(ymds_regexes))
  
  # populate each column
  for (col in seq_along(ymds_regexes)) {
    curr_regex <- ymds_regexes[[col]]
    ymds[, col] <- str_match(medline_date, curr_regex)[, 2]
  }
  
  return(ymds)
}

extract_md <- function(pub_season) {
  md <- matrix(NA, nrow=length(pub_season), ncol=2)
  seasons <- c("Spring", "Summer", "Fall", "Winter")
  season_starts <- list(c(3, 1), c(6, 1), c(9, 1), c(12, 1))
  
  for (i in seq_along(seasons)) {
    idx <- which(pub_season == seasons[i])
    md[idx, ] <- matrix(
      rep(season_starts[[i]], length(idx)),
      nrow=length(idx),
      byrow=TRUE
    )
  }
  
  return(md)
}

# a function to help keep track of imputed/inferred date info
count_ymds <- function(df) {
  row <- list()
  for (col in c('pub_year', 'pub_month', 'pub_day', 'pub_season')) {
    row[[col]] <- sum(!is.na(df[, col]))
  }
  return(row)
}


# --- scratch
journal_df <- read.csv('data/journal_df.csv')
author_df <- read.csv('data/author_df.csv')
article_df <- read.csv('data/article_df.csv')
article_author_df <- read.csv('data/article_author_df.csv')
results_df <- merge(article_df, journal_df, by=c('journal_id'))
# results_df <- merge(results_df, author_df, by=c('author_id'))
results_df <- read.csv('data/article_df.csv')



batch_size <- 80000 #1024
result <- dbSendQuery(con, sql_query)

# TODO: initialize vector of counts to all zeros... maybe something simpler?
ex_ts_counts <- c('pub_day', 'pub_month', 'pub_year', 'pub_season')

n_ts_from_day <- 0
n_ts_from_month <- 0
n_ts_from_year <- 0
n_ts_from_season <- 0
while (!dbHasCompleted(result)) {
  results_df <- dbFetch(result, n = batch_size)
  if (nrow(results_df) == 0) {
    break
  }
  
  # ymds pre-count
  ymd_counts <- count_ymds(results_df)
  
  # get what we can from medline_date
  ymds <- extract_ymds(results_df$medline_date)
  results_df <- results_df %>%
    mutate(
      pub_year = ifelse(is.na(pub_year), ymds[, 1], pub_year),
      pub_month = ifelse(is.na(pub_month), ymds[, 2], pub_month),
      pub_day = ifelse(is.na(pub_day), ymds[, 3], pub_day),
      pub_season = ifelse(is.na(pub_season), ymds[, 4], pub_season)
    )
  
  
  # apply title case to the month and season
  results_df$pub_month <- str_to_title(results_df$pub_month)
  results_df$pub_season <- str_to_title(results_df$pub_season)
  
  # check results
  logf('Extracted YMDS from medline_date:')
  ymd_counts <- rbind(ymd_counts, parse_medline=count_ymds(results_df))
  print(ymd_counts)
  
  # check hierarchy of YMDS
  has_day <- !(is.na(results_df$pub_day))
  has_month <- !(is.na(results_df$pub_month))
  has_year <- !(is.na(results_df$pub_year))
  has_season <- !(is.na(results_df$pub_season))
  
  # should be zero
  tmp <- sum(has_season & !has_year)
  if (tmp) {
    logf('Encountered Season without Year: %s', tmp)
  }
  tmp <- sum(has_month & !has_year)
  if (tmp) {
    logf('Encountered Month without Year: %s', tmp)
  }
  tmp <- sum(has_day & !has_year)
  if (tmp) {
    logf('Encountered Day without Year: %s', tmp)
  }
  tmp <- sum(has_day & !has_month)
  if (tmp) {
    logf('Encountered Day without Month: %s', tmp)
  }
  tmp <- sum(has_season & has_day)
  if (tmp) {
    logf('Encountered Season with Day: %s', tmp)
  }
  tmp <- sum(has_season & has_month)
  if (tmp) {
    logf('Encountered Season with Month: %s', tmp)
  }
  
  # check for opportunities to infer dates from Season and Year
  tmp <- sum(has_season & (!has_month | !has_day))
  if (tmp) {
    logf('Encountered %s opportunities to infer Month/Day from Season.', tmp)
  }
  md <- extract_md(results_df$pub_season)
  results_df <- results_df %>%
    mutate(
      pub_month = ifelse(is.na(pub_month), md[, 1], pub_month),
      pub_day = ifelse(is.na(pub_day), md[, 2], pub_day)
    )
  
  logf('Extracted MD from Season:')
  ymd_counts <- rbind(ymd_counts, season_start=count_ymds(results_df))
  print(ymd_counts)
  
  # reset counts
  has_day <- !(is.na(results_df$pub_day))
  has_month <- !(is.na(results_df$pub_month))
  has_year <- !(is.na(results_df$pub_year))
  has_season <- !(is.na(results_df$pub_season))
  
  tmp <- sum(has_year & (!has_month & !has_day))
  if (tmp) {
    logf('Encountered %s opportunities to infer Month/Day from Year', tmp)
  }
  results_df <- results_df %>%
    mutate(
      pub_month = ifelse(is.na(pub_month), 1, pub_month),
      pub_day = ifelse(is.na(pub_day), 1, pub_day)
    )
  logf('Inferred MD from Year:')
  ymd_counts <- rbind(ymd_counts, year_start=count_ymds(results_df))
  print(ymd_counts)
  
  # ymd_counts:
  #   pub_year pub_month pub_day pub_season
  #   ymd_counts    27367    21803     7494    86        
  #   parse_medline 30000    24425     7586    89        
  #   season_start  30000    24514     7675    89        
  #   year_start    30000    30000     30000   89        
  
  # derive some actual timestamps from date fields
  results_df$pub_date <- NA
  
  # fill in the date as well as we can with YMD
  results_df <- results_df %>%
    mutate(
      pub_date = case_when(
        !is.na(pub_year) & !is.na(pub_month) & !is.na(pub_day) ~ ymd(paste(pub_year, pub_month, pub_day, sep = "-")),
        !is.na(pub_year) & !is.na(pub_month)                   ~ ymd(paste(pub_year, pub_month, "01", sep = "-")),
        !is.na(pub_year)                                       ~ ymd(paste(pub_year, "01", "01", sep = "-")),
        TRUE ~ NA
      )
    )
}

# close the result set
dbClearResult(result)
