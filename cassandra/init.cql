CREATE KEYSPACE IF NOT EXISTS wiki_namespace
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE wiki_namespace;


CREATE TABLE IF NOT EXISTS edits (
    user_id TEXT,
    domain TEXT,
    created_at TIMESTAMP,
    page_title TEXT,
    PRIMARY KEY ((user_id), created_at, domain)
) WITH CLUSTERING ORDER BY (created_at DESC, domain ASC);

CREATE INDEX IF NOT EXISTS edits_domain_idx ON edits (domain);
CREATE INDEX IF NOT EXISTS edits_page_title_idx ON edits (page_title);