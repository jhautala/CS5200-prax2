CREATE TABLE author(
    author_id INTEGER PRIMARY KEY AUTOINCREMENT,
    last_name TEXT,
    first_name TEXT,
    initials TEXT,
    suffix TEXT,
    collective_name TEXT,
    affiliation TEXT
);

CREATE TABLE journal(
    journal_id INTEGER PRIMARY KEY AUTOINCREMENT,
    issn TEXT NOT NULL,
    issn_type TEXT,
    title TEXT,
    iso_abbrev TEXT
);

CREATE TABLE article(
    pmid INTEGER PRIMARY KEY,
    journal_id INTEGER NOT NULL,
    article_title TEXT,
    cited_medium TEXT,
    journal_volume TEXT,
    journal_issue TEXT,
    pub_year INTEGER,
    pub_month INTEGER,
    pub_day INTEGER,
    pub_season TEXT,
    medline_date TEXT,
    author_list_complete BOOLEAN CHECK (author_list_complete IN (0, 1)),
    FOREIGN KEY (journal_id) REFERENCES journal(journal_id)
);

CREATE TABLE article_author(
    pmid INTEGER NOT NULL,
    author_id INTEGER NOT NULL,
    is_valid BOOLEAN CHECK (is_valid IN (0, 1)),
    PRIMARY KEY (pmid, author_id),
    FOREIGN KEY (pmid) REFERENCES article(pmid),
    FOREIGN KEY (author_id) REFERENCES author(author_id)
);
