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
optional_range <- function(s) {
  return(sprintf('(%s)(-%s)?', s, s))
}

year_regex <- regex(optional_range('\\d{4}'))
month_regex <- regex(optional_range(paste(month.abb, collapse='|')), ignore_case=TRUE)
day_regex <- regex('(?<!\\d)(\\d{1,2})(-\\d{1,2})?(?!\\d)')
season_regex <- regex(optional_range('Spring|Summer|Winter|Fall'), ignore_case=TRUE)
ymds_regexes <- c(year_regex, month_regex, day_regex, season_regex)

extract_ymds <- function(medline_dates, pub_year, pub_month, pb_day, pub_season) {
  # create a matrix, populated with existing years and seasons
  ymds <- cbind(pub_year, pub_month, pub_day, pub_season)
  for (col in seq_along(ymds_regexes)) {
    curr_regex <- ymds_regexes[[col]]
    capture_result <- str_match(medline_dates, curr_regex)
    is_valid <- is.na(ymds[, col]) & !is.na(capture_result[, 1])
    ymds[is_valid, col] <- capture_result[is_valid, 2]
  }
  
  return(ymds)
}

# function to extract timestamps as start of meteorological season
season_dates <- function(pub_year, pub_season) {
  start_date <- switch(
    pub_season,
    "Spring" = ymd(paste(pub_year, "03", "01", sep = "-")),
    "Summer" = ymd(paste(pub_year, "06", "01", sep = "-")),
    "Fall" = ymd(paste(pub_year, "09", "01", sep = "-")),
    "Winter" = ymd(paste(pub_year, "12", "01", sep = "-"))
  )
  return(start_date)
}

# vectorized
season_dates_vec <- Vectorize(season_dates, vectorize.args = c("pub_year", "pub_season"))




# Usage example
years <- c(2021, 2022, 2023)
seasons <- c("Spring", "Summer", "Fall")
dates <- season_dates_vec(years, seasons)

# Convert the result into a data frame with columns "start_date", "mid_date", "end_date"
dates_df <- data.frame(t(matrix(dates, nrow = 3, dimnames = list(c("start_date", "mid_date", "end_date")))))

count_date_fields <- function(df) {
  has_day <- !(is.na(df$pub_day))
  has_month <- !(is.na(df$pub_month))
  has_year <- !(is.na(df$pub_year))
  has_season <- !(is.na(df$pub_season))
  print(paste(
    'has day:', sum(has_day), ';',
    'has month:', sum(has_month), ';',
    'has years:', sum(has_year), ';',
    'has season:', sum(has_season)
  ))
}
count_date_fields(article_df)
count_date_fields(results_df)

has_month <- !(is.na(article_df$pub_month))
has_year <- !(is.na(article_df$pub_year))
has_season <- !(is.na(article_df$pub_season))
print(paste(
  'has years:', sum(has_year), ';',
  'has month:', sum(has_month), ';',
  'has season:', sum(has_season)
))
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
  
  # get what we can from medline_date
  results_df[, c('pub_year', 'pub_month', 'pub_day', 'pub_season')] <- extract_ymds(
    results_df$medline_date,
    results_df$pub_year,
    results_df$pub_month,
    results_df$pub_day,
    results_df$pub_season
  )
  
  # apply title case to the month and season
  results_df$pub_month <- str_to_title(results_df$pub_month)
  results_df$pub_season <- str_to_title(results_df$pub_season)
  
  # derive some actual timestamps from date fields
  results_df$pub_date <- NA
  results_df$pub_date <- as.Date(results_df$pub_date)
  
  # fill in the date as well as we can with YMD
  results_df <- results_df %>%
    mutate(
      pub_date = case_when(
        !is.na(pub_day) ~ ymd(paste(pub_year, pub_month, pub_day, sep = "-")),
        !is.na(pub_month) ~ ymd(paste(pub_year, pub_month, "01", sep = "-")),
        !is.na(pub_year) ~ ymd(paste(pub_year, "01", "01", sep = "-")),
        TRUE ~ NA
      )
    )
  
  # also fill in date with season
  # TODO: finish adapting this to our data
  years <- c(2021, 2022, 2023)
  seasons <- c("Spring", "Summer", "Fall")
  dates <- season_dates_vec(years, seasons)
  
  # Convert the result into a data frame with columns "start_date", "mid_date", "end_date"
  dates_df <- data.frame(t(matrix(dates, nrow = 3, dimnames = list(c("start_date", "mid_date", "end_date")))))
}

# close the result set
dbClearResult(result)
