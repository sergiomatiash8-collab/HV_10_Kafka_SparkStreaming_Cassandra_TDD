CREATE KEYSPACE IF NOT EXISTS wiki_namespace 
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS wiki_namespace.edits (
    id          UUID     PRIMARY KEY,
    page_title  TEXT,
    user_text   TEXT,
    dt          TIMESTAMP
);