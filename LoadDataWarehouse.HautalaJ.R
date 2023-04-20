# NOTE: This ETL script is re-entrant for a single extract, but since it reinitializes
#       the OLAP DB, it is not suitable for deployment where the OLAP DB needs to be
#       "long-lived" (i.e. iteratively appended to over time).

# IMPORTANT: Due to inequality of NULL in SQL, the intended unique constraint for
#   `dim.author` is not fully enforced at the database level.

# Acknowledgments: I used GPT-4 extensively during development of this script, primarily
#   as a way to sift through the R documentation and discover language/library features.
#   It proved indispensable for parsing stacktraces (or "tracebacks") and learning more
#   about R syntax, particularly around vectorized operations, dealing with named lists,
#   and some common R functions (e.g. `lapply`, `(r|c)bind`, etc.).
#
#   The main thing I would have done differently without feedback from GPT-4 is that I
#   would have avoided some dependencies, specifically 'dplyr' and 'lubridate'.
#   I have a strong bias to avoid dependencies in favor of "base R", but after seeing
#   examples of how to use some of the functions and operators provided therein, I was
#   convinced of their value. To be fair most of my concerns (e.g. namespace masking)
#   are fairly easily addressed, and this is part of the process of gaining familiarity
#   with a language and conventional libraries.
#
#   More specifically, I would not have used the `mutate` function without GPT's
#   prompting (so to speak), and though I didn't take any code directly from GPT unmodified,
#   this line in particular would not exist without the responses I got from GPT:
#
#   `results_df <- results_df %>% mutate(across(names(ymds), ~coalesce(., ymds[[cur_column()]])))`
#
#   As a basis for comparison, the reader may review the implementation of "LoadXML2DB.HautalaJ.R",
#   paying particular attention to the more verbose/explicit, and less-than-elegant statements
#   I used there to create vectors, lists and data frames. To some extent, each occurrence of a
#   column name or table name in the code represents an increased liability in the form of roadblocks
#   to change/adaptation, so though it is more complicated to use more generalized/abstract/dynamic
#   coding practices, it is much easier to adapt to schema changes if we only have to change a
#   variable names or string values in a few places.

do_http_cache <- TRUE
batch_size <- 4096

# simple logging
logf <- function(s, ...) {
  print(Sys.time())
  cat(sprintf(paste(s, "\n", sep=""), ...))
}

# install dependencies
deps <- c('DBI', 'RSQLite', 'RMariaDB', 'stringr', 'tidyverse', 'lubridate', 'gsubfn')
missingDeps <- setdiff(deps, installed.packages()[,"Package"])
if (length(missingDeps)) {
  install.packages(missingDeps)
}

library(DBI)
library(RSQLite)
library(RMariaDB)
library(stringr)
library(tidyverse)
library(lubridate)
library(gsubfn)

# confirm dependencies
# This script was developed with the following versions:
#   * R: R version 4.2.1 (2022-06-23)
#   * DBI: 1.1.3
#   * RSQLite: 2.3.0
#   * RMariaDB: 1.2.2
#   * stringr: 1.5.0
#   * tidyverse: 2.0.0
#   * lubridate: 1.9.2
#   * gsubfn: 0.7
print(sprintf('R: %s', R.version.string))
for (dep in deps) {
  print(sprintf('%s: %s', dep, packageVersion(dep)))
}

# IMPORTANT: Initial DB setup requires at least three things:
#   
#   1. `db_user`: the name of the root user account for your DB server
#   2. `db_pass`: the password for your root user
# 
# It is simplest to create a YAML credentials file named 'db_cred.yml' in the
# working directory, alongside this script.
# 
# NOTE:
#   * It is not strictly required that you use your root account, but significant privileges are required.
#   * Replace asterisks with your names and passwords.
#   * The final line in the file must be empty.
# 
# ### YAML file structure
# ```yaml
# db_admin:
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
on.exit(dbDisconnect(oltp_dbcon), add=TRUE)

# connect to the data warehouse
olap_dbcon <- dbConnect(
  RMariaDB::MariaDB(),
  user=db_user,
  password=db_pass,
  host=db_host,
  port=db_port,
  load_data_local_infile=TRUE
)
on.exit(dbDisconnect(olap_dbcon), add=TRUE)

# read the schema file
# NOTE: this will clear out and rebuild 'fact' and 'dim' schemas
# TODO: add unique constraint to fact table?
schema_sql <- readChar(olap_schema_file, file.info(olap_schema_file)$size)
sqls_statements <- strsplit(schema_sql, '\\s*;\\s*')[[1]]

# execute the SQL to (re/)create schema
for (sql in sqls_statements) {
  dbExecute(olap_dbcon, paste0(sql, ';'))
}

# --- some utility functions for parsing MedlineDate
# a function to help keep track of imputed/inferred date info
count_ymds <- function(df) {
  row <- list()
  for (col in c('pub_year', 'pub_month', 'pub_day', 'pub_season')) {
    row[[col]] <- sum(!is.na(df[, col]))
  }
  return(row)
}

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
  # create a data frame to hold the extracted YMDS
  ymds <- data.frame(
    pub_year = replicate(length(medline_date), NA_integer_),
    pub_month = replicate(length(medline_date), NA_integer_),
    pub_day = replicate(length(medline_date), NA_integer_),
    pub_season = replicate(length(medline_date), NA_character_),
    stringsAsFactors = FALSE
  )
  
  # populate each column
  for (col in seq_along(ymds_regexes)) {
    curr_regex <- ymds_regexes[[col]]
    vals <- str_match(medline_date, curr_regex)[, 2]
    
    if (col == 2) {
      # standardize month abbreviations
      vals <- str_to_title(vals)
      
      # convert months to integer
      month_map <- setNames(1:12, month.abb)
      vals <- unname(month_map[vals])
    } else if (col == 4) {
      # do nothing
      # NOTE: capture groups are already strings (seasons are strings)
    } else {
      # convert year and day to integer
      vals <- as.integer(vals)
    }
    
    ymds[, col] <- vals
  }
  
  return(ymds)
}

extract_md <- function(pub_season) {
  md <- matrix(nrow=length(pub_season), ncol=2)
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

simpleErrorLogger = function(cond) {
  sink(stderr())
  on.exit(sink(NULL))
  message(sprintf('Execution failed due to unexpected %s:\n%s', class(cond)[1], cond))
}

# util function for inserting dimensions the into OLAP DB
# NOTE: Since there is no way to batch insert and efficiently retrieve the surrogate
#       keys, we must insert each row, one at a time.
insert_dim <- function(
    dbcon,
    table_name,
    surrogate_key_colname,
    natural_key_colnames,
    dim_df
) {
  # keep track of foreign keys
  row_ids <- integer(nrow(dim_df))
  new_rows <- logical(nrow(dim_df))
  extant_rows <- new.env()
  
  # loop over each row to get correct auto-generated IDs
  col_names <- colnames(dim_df)
  for (i in 1:nrow(dim_df)) {
    row_data <- dim_df[i, ]
    natural_key <- row_data[, natural_key_colnames]
    row_key <- paste(ifelse(is.na(natural_key), "\x1F", as.character(natural_key)), collapse='\x1E')
    if (exists(row_key, extant_rows)) {
      new_rows[i] <- FALSE
      row_ids[i] <- extant_rows[[row_key]]
      next
    }
    
    # check for extant dim row
    key_filter <- list()
    params <- list()
    for (col in natural_key_colnames) {
      if (is.na(row_data[, col])) {
        key_filter[[length(key_filter) + 1]] <- paste(col, 'IS NULL')
      } else {
        key_filter[[length(key_filter) + 1]] <- paste0(col, '=?')
        params[[length(params) + 1]] <- row_data[, col]
      }
    }
    select_query <- sprintf(
      "SELECT %s FROM %s WHERE %s;",
      surrogate_key_colname,
      table_name,
      paste(key_filter, collapse = " AND ")
    )
    if (length(params)) {
      select_extant_statement <- dbSendQuery(dbcon, select_query)
      dbBind(select_extant_statement, params)
      extant_id <- dbFetch(select_extant_statement)
      dbClearResult(select_extant_statement)
    } else {
      # skip rows with all null
      new_rows[[i]] <- FALSE # not a new row
      row_ids[[i]] <- NA_integer_ # null pointer for foreign key
      extant_rows[[row_key]] <- row_ids[i] # skip subsequent occurrences
      next
    }
    
    # insert as needed
    if (nrow(extant_id) > 0) {
      new_rows[i] <- FALSE
      row_ids[i] <- as.integer(extant_id[[1]])
    } else {
      insert_query <- paste0(
        sprintf("INSERT INTO %s ", table_name),
        "(", paste(col_names, collapse=", "), ") ",
        "VALUES ",
        "(", paste(rep("?", length(col_names)), collapse=", "), ");"
      )
      insert_statement <- dbSendStatement(dbcon, insert_query)
      dbBind(insert_statement, unname(as.list(row_data)))
      dbGetRowsAffected(insert_statement)
      dbClearResult(insert_statement)
      
      # get the ID for this insert
      select_query <- "SELECT LAST_INSERT_ID();"
      inserted_id <- dbGetQuery(dbcon, select_query)
      new_rows[i] <- TRUE
      row_ids[i] <- as.integer(inserted_id[[1]])
    }
    
    extant_rows[[row_key]] <- row_ids[i]
  }
  
  return(list(row_ids, new_rows))
}

insert_batch = function(results_df) {
  tryCatch(
    {
      # start a new transaction
      dbBegin(olap_dbcon)
      
      # --- insert dimensions first to get foreign keys
      pub_date_dim_df <- data.frame(results_df[, c(
        'pub_date',
        'pub_year',
        'pub_quarter',
        'pub_month',
        'pub_day',
        'pub_season',
        'imputed_year',
        'imputed_month',
        'imputed_day',
        'imputed_season',
        'month_from_year',
        'month_from_season',
        'day_from_year',
        'day_from_month',
        'day_from_season'
      )])
      list[pub_date_dim_id, new_pub_dates] <- insert_dim(
        olap_dbcon,
        'dim.pub_date',
        'pub_date_dim_id',
        c(
          'pub_date',
          'pub_season',
          'imputed_year',
          'imputed_month',
          'imputed_day',
          'imputed_season',
          'month_from_year',
          'month_from_season',
          'day_from_year',
          'day_from_month',
          'day_from_season'
        ),
        pub_date_dim_df
      )
      
      journal_dim_df <- results_df %>%
        select(journal_id, issn, issn_type, journal_title, iso_abbrev) %>%
        rename(title = journal_title)
      list[journal_dim_id, new_journals] <- insert_dim(
        olap_dbcon,
        'dim.journal',
        'journal_dim_id',
        c('journal_id'),
        journal_dim_df
      )
      
      article_dim_df <- data.frame(results_df[, c(
        'pmid',
        'article_title',
        'cited_medium',
        'journal_volume',
        'journal_issue',
        'author_list_complete'
      )])
      # NOTE: This wasn't working and I have no idea why.
      # article_dim_id <- insert_dim(
      #   olap_dbcon,
      #   'dim.article',
      #   'article_dim_id',
      #   c('pmid'),
      #   article_dim_df,
      # )
      # NOTE: I even tried explicitly naming each argument, but that didn't work either...
      # article_dim_id <- insert_dim(
      #   dbcon=olap_dbcon,
      #   table_name='dim.article',
      #   surrogate_key_colname='article_dim_id',
      #   natural_key_colnames=c('pmid'),
      #   dim_df=article_dim_df,
      # )
      # NOTE: this does work... TODO: research this error to understand R better
      dbcon <- olap_dbcon
      table_name <- 'dim.article'
      surrogate_key_colname <- 'article_dim_id'
      natural_key_colnames <- c('pmid')
      dim_df <- article_dim_df
      list[article_dim_id, new_articles] <- insert_dim(
        dbcon,
        table_name,
        surrogate_key_colname,
        natural_key_colnames,
        dim_df
      )
      
      author_dim_df <- data.frame(results_df[, c(
        'author_id',
        'last_name',
        'first_name',
        'initials',
        'suffix',
        'collective_name',
        'affiliation'
      )])
      
      list[author_dim_id, new_authors] <- insert_dim(
        olap_dbcon,
        'dim.author',
        'author_dim_id',
        colnames(author_dim_df),
        author_dim_df
      )
      
      # --- create fact data frame
      article_author_fact_df <- as.data.frame(
        cbind(
          pub_date_dim_id,
          journal_dim_id,
          article_dim_id,
          author_dim_id,
          is_valid=as.logical(results_df$is_valid)
        )
      )
      
      # insert facts
      dbAppendTable(
        olap_dbcon,
        Id(schema='fact', table='article_author'),
        article_author_fact_df,
        safe=FALSE
      )
      dbCommit(olap_dbcon)
      
      # collect insert results
      row_counts <- list(
        new_pub_dates=sum(new_pub_dates),
        new_journals=sum(new_journals),
        new_articles=sum(new_articles),
        new_authors=sum(new_authors),
        new_facts=nrow(article_author_fact_df)
      )
      
      # log progress
      logf('Inserted new facts:')
      counts_df <- setNames(do.call(rbind, lapply(row_counts, as.data.frame)), "count")
      print(counts_df)
      
      return(row_counts)
    },
    warning = function(cond) {
      message('Rolling back the transaction')
      dbRollback(olap_dbcon)
      simpleErrorLogger(cond)
      return(NULL)
    },
    error = function(cond) {
      message('Rolling back the transaction')
      dbRollback(olap_dbcon)
      simpleErrorLogger(cond)
      return(NULL)
    }
  )
}

# Query OLTP DB and iterate over results
sql_query <- (
    'SELECT a.pmid,
            a.article_title,
            a.cited_medium,
            a.journal_volume,
            a.journal_issue,
            a.pub_year,
            a.pub_month,
            a.pub_day,
            a.pub_season,
            a.medline_date,
            a.author_list_complete,
            j.journal_id,
            j.issn,
            j.issn_type,
            j.title AS journal_title,
            j.iso_abbrev,
            au.author_id,
            au.last_name,
            au.first_name,
            au.initials,
            au.suffix,
            au.collective_name,
            au.affiliation,
            aa.is_valid
       FROM article AS a
  LEFT JOIN journal AS j ON a.journal_id = j.journal_id
  LEFT JOIN article_author AS aa ON a.pmid = aa.pmid
  LEFT JOIN author AS au ON aa.author_id = au.author_id
')

# get batches from OLTP DB
result <- dbSendQuery(oltp_dbcon, sql_query)
new_row_counts <- list()
batch_i <- 1
got_error <- FALSE
all_ymds_counts <- NULL
while (!dbHasCompleted(result)) {
  results_df <- dbFetch(result, n=batch_size)
  if (nrow(results_df) == 0) {
    break
  }
  
  # initialize imputed value flags to FALSE
  # NOTE: quarter is always imputed
  results_df$imputed_year <- FALSE
  results_df$imputed_month <- FALSE
  results_df$imputed_day <- FALSE
  results_df$imputed_season <- FALSE
  results_df$month_from_year <- FALSE
  results_df$month_from_season <- FALSE
  results_df$day_from_year <- FALSE
  results_df$day_from_month <- FALSE
  results_df$day_from_season <- FALSE
  
  # --- extract MedlineDate first
  # create YMDS masks
  has_year <- !(is.na(results_df$pub_year))
  has_month <- !(is.na(results_df$pub_month))
  has_day <- !(is.na(results_df$pub_day))
  has_season <- !(is.na(results_df$pub_season))
  
  # track counts of YMDS fields
  # TODO: Address redundancy around so many YMDS masks?
  #       This function makes its own...
  ymds_counts <- count_ymds(results_df)
  
  # --- get what we can from medline_date
  ymds <- extract_ymds(results_df$medline_date)
  
  # copy those into the results data frame if NA
  # NOTE: this terse syntax depends on column names
  results_df <- results_df %>% mutate(across(names(ymds), ~coalesce(., ymds[[cur_column()]])))
  
  # store the results of this step
  ymds_counts <- rbind(initial_counts=ymds_counts, parse_medline=count_ymds(results_df))
  
  # record imputed values
  results_df[(!has_year & !is.na(results_df$pub_year)), 'imputed_year'] <- TRUE
  results_df[(!has_month & !is.na(results_df$pub_month)), 'imputed_month'] <- TRUE
  results_df[(!has_day & !is.na(results_df$pub_day)), 'imputed_day'] <- TRUE
  results_df[(!has_season & !is.na(results_df$pub_season)), 'imputed_season'] <- TRUE
  
  # reset YMDS masks
  has_year <- !(is.na(results_df$pub_year))
  has_month <- !(is.na(results_df$pub_month))
  has_day <- !(is.na(results_df$pub_day))
  has_season <- !(is.na(results_df$pub_season))
  
  # check hierarchy of YMDS (these should all be zero)
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
  
  # --- check for opportunities to infer day from Month
  # NOTE: We do this first (after medline date) so we can keep track of
  #       where each field was logically imputed from.
  tmp <- sum(has_month & !has_day)
  if (tmp) {
    logf('Encountered %s opportunities to infer Day from Month', tmp)
  }
  results_df <- results_df %>%
    mutate(
      pub_day = ifelse(!is.na(pub_month) & is.na(pub_day), 1, pub_day)
    )
  ymds_counts <- rbind(ymds_counts, day_from_month=count_ymds(results_df))
  
  # record inferred values
  day_from_month_mask <- (!has_day & !is.na(results_df$pub_day))
  results_df[day_from_month_mask, 'day_from_month'] <- TRUE
  results_df[day_from_month_mask, 'imputed_day'] <- TRUE
  
  # reset YMDS masks
  has_year <- !(is.na(results_df$pub_year))
  has_month <- !(is.na(results_df$pub_month))
  has_day <- !(is.na(results_df$pub_day))
  has_season <- !(is.na(results_df$pub_season))
  
  # --- check for opportunities to infer day/month from Season
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
  ymds_counts <- rbind(ymds_counts, season_start=count_ymds(results_df))
  
  # record inferred values
  month_from_season_mask <- (!has_month & !is.na(results_df$pub_month))
  day_from_season_mask <- (!has_day & !is.na(results_df$pub_day))
  results_df[month_from_season_mask, 'imputed_month'] <- TRUE
  results_df[month_from_season_mask, 'month_from_season'] <- TRUE
  results_df[day_from_season_mask, 'day_from_season'] <- TRUE
  results_df[day_from_season_mask, 'imputed_day'] <- TRUE
  
  # reset YMDS masks
  has_year <- !(is.na(results_df$pub_year))
  has_month <- !(is.na(results_df$pub_month))
  has_day <- !(is.na(results_df$pub_day))
  has_season <- !(is.na(results_df$pub_season))
  
  # --- check for opportunities to infer day/month from Year
  tmp <- sum(has_year & (!has_month & !has_day))
  if (tmp) {
    logf('Encountered %s opportunities to infer Month/Day from Year', tmp)
  }
  results_df <- results_df %>%
    mutate(
      pub_month = ifelse(!is.na(pub_year) & is.na(pub_month), 1, pub_month),
      pub_day = ifelse(!is.na(pub_month) & is.na(pub_day), 1, pub_day)
    )
  ymds_counts <- rbind(ymds_counts, year_start=count_ymds(results_df))
  
  # record inferred values
  month_from_year_mask <- (!has_month & !is.na(results_df$pub_month))
  day_from_year_mask <- (!has_day & !is.na(results_df$pub_day))
  results_df[month_from_year_mask, 'month_from_year'] <- TRUE
  results_df[month_from_year_mask, 'imputed_month'] <- TRUE
  results_df[day_from_year_mask, 'day_from_year'] <- TRUE
  results_df[day_from_year_mask, 'imputed_day'] <- TRUE
  
  ymds_counts <- as.data.frame(ymds_counts)
  ymds_counts$batch <- batch_i
  ymds_counts$step <- factor(row.names(ymds_counts), levels=row.names(ymds_counts), ordered=TRUE)
  row.names(ymds_counts) <- NULL
  ymds_counts <- ymds_counts[, c('batch', 'step', 'pub_year', 'pub_month', 'pub_day', 'pub_season')]
  if (is.null(all_ymds_counts)) {
    all_ymds_counts <- ymds_counts
  } else {
    all_ymds_counts <- rbind(all_ymds_counts, ymds_counts)
  }
  
  # fill in the date as well as we can with YMD
  results_df$pub_date <- NA
  results_df <- results_df %>%
    mutate(
      pub_date = case_when(
        !is.na(pub_year) & !is.na(pub_month) & !is.na(pub_day) ~ ymd(paste(pub_year, pub_month, pub_day, sep = "-")),
        !is.na(pub_year) & !is.na(pub_month)                   ~ ymd(paste(pub_year, pub_month, "01", sep = "-")),
        !is.na(pub_year)                                       ~ ymd(paste(pub_year, "01", "01", sep = "-")),
        TRUE ~ NA
      ),
      pub_quarter = case_when(
        !is.na(pub_month) ~ifelse(
          pub_month < 4, 1, ifelse(
            pub_month < 7, 2, ifelse(
              pub_month < 10, 3, ifelse(
                pub_month < 13, 4, NA_integer_
              )
            )))
      )
    )
  
  # insert batch into OLAP DB
  curr_row_counts <- insert_batch(results_df)
  if (is.null(curr_row_counts)) {
    got_error <- TRUE
    break
  }
  new_row_counts[[paste('batch ', batch_i)]] <- list(curr_row_counts)
  batch_i <- batch_i + 1
}

# close the result set
dbClearResult(result)

logf(if (got_error) 'Ingestion Failed!' else 'Finished ingestion!')

# Inferred YMDS:
#                pub_year pub_month pub_day pub_season
# initial_counts 68818    55123     18881   148       
# parse_medline  75826    62105     19134   151       
# season_start   75826    62256     19285   151       
# year_start     75826    75826     75826   151       
all_ymds_counts$pub_year <- sapply(all_ymds_counts$pub_year, as.numeric)
all_ymds_counts$pub_month <- sapply(all_ymds_counts$pub_month, as.numeric)
all_ymds_counts$pub_day <- sapply(all_ymds_counts$pub_day, as.numeric)
all_ymds_counts$pub_season <- sapply(all_ymds_counts$pub_season, as.numeric)
agg_ymds_count <- aggregate(. ~ step, data = all_ymds_counts, FUN = sum, na.rm = TRUE)
print(agg_ymds_count[, -which(names(agg_ymds_count) %in% c('batch'))])

counts_df <- do.call(rbind, lapply(new_row_counts, as.data.frame))
print(counts_df)

dbDisconnect(oltp_dbcon)
dbDisconnect(olap_dbcon)
