-- Cassandra Schema Initialization
-- Створює keyspace та таблицю для Wikipedia edits

CREATE KEYSPACE IF NOT EXISTS wiki_namespace
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE wiki_namespace;

CREATE TABLE IF NOT EXISTS edits (
    id UUID PRIMARY KEY,
    page_title TEXT,
    user_text TEXT,
    dt TIMESTAMP
);

CREATE INDEX IF NOT EXISTS edits_dt_idx ON edits (dt);
CREATE INDEX IF NOT EXISTS edits_user_idx ON edits (user_text);