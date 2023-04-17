-- NOTE: This setting does present a security concern.
SET GLOBAL local_infile = true;

-- freshly create our schemas
DROP SCHEMA IF EXISTS fact;
DROP SCHEMA IF EXISTS dim;

CREATE SCHEMA dim;
CREATE SCHEMA fact;

-- create our dimension tables
CREATE TABLE dim.pub_date(
    pub_date_dim_id INTEGER PRIMARY KEY AUTO_INCREMENT,
    pub_date DATE,
    pub_year INTEGER,
    pub_quarter TINYINT,
    pub_month TINYINT,
    pub_day TINYINT,
    pub_season VARCHAR(6),
    imputed_year BOOLEAN,
    imputed_month BOOLEAN,
    imputed_day BOOLEAN,
    imputed_season BOOLEAN,
    UNIQUE (
        pub_date,
        pub_season,
        imputed_year,
        imputed_month,
        imputed_day,
        imputed_season
    )
);

CREATE TABLE dim.journal(
    journal_dim_id INTEGER PRIMARY KEY AUTO_INCREMENT,
    journal_id INTEGER,
    issn VARCHAR(9) NOT NULL,
    issn_type TEXT,
    title TEXT,
    iso_abbrev TEXT,
    UNIQUE(issn)
);

CREATE TABLE dim.article(
    article_dim_id INTEGER PRIMARY KEY AUTO_INCREMENT,
    pmid INTEGER NOT NULL,
    article_title TEXT,
    cited_medium TEXT,
    journal_volume TEXT,
    journal_issue TEXT,
    author_list_complete BOOLEAN,
    UNIQUE(pmid)
);

CREATE TABLE dim.author(
    author_dim_id INTEGER PRIMARY KEY AUTO_INCREMENT,
    author_id INTEGER,
    last_name VARCHAR(60),
    first_name VARCHAR(60),
    initials VARCHAR(16),
    suffix VARCHAR(10),
    collective_name VARCHAR(100),
    affiliation VARCHAR(400),
    UNIQUE(last_name, first_name, initials, suffix, collective_name, affiliation)
);

-- create our fact table
CREATE TABLE fact.article_author(
    article_author_id INTEGER PRIMARY KEY AUTO_INCREMENT,
    pub_date_dim_id INTEGER NOT NULL,
    journal_dim_id INTEGER NOT NULL,
    article_dim_id INTEGER NOT NULL,
    author_dim_id INTEGER NOT NULL,
    is_valid BOOLEAN,
    FOREIGN KEY (pub_date_dim_id) REFERENCES dim.pub_date(pub_date_dim_id),
    FOREIGN KEY (journal_dim_id) REFERENCES dim.journal(journal_dim_id),
    FOREIGN KEY (article_dim_id) REFERENCES dim.article(article_dim_id),
    FOREIGN KEY (author_dim_id) REFERENCES dim.author(author_dim_id)
);

-- set the current DB
USE fact;
