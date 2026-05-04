USE wiki_namespace;

DROP TABLE IF EXISTS edits;

CREATE TABLE edits (
    user_id TEXT,
    domain TEXT,
    page_title TEXT,
    created_at TIMESTAMP,
    user_is_bot BOOLEAN,
    PRIMARY KEY (domain, created_at, user_id)
) WITH CLUSTERING ORDER BY (created_at DESC, user_id ASC);

CREATE INDEX IF NOT EXISTS edits_page_title_idx ON edits (page_title);