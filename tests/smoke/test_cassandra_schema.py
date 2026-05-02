"""
Smoke test: Перевірка схеми Cassandra.
"""

import pytest
from cassandra.cluster import Cluster
import time


def wait_for_cassandra(max_retries=10):
    """Чекає Cassandra."""
    for attempt in range(max_retries):
        try:
            cluster = Cluster(['cassandra'], port=9042)
            session = cluster.connect()
            cluster.shutdown()
            return True
        except Exception:
            if attempt < max_retries - 1:
                time.sleep(5)
    return False


def test_cassandra_keyspace_exists():
    """Перевіряє існування keyspace."""
    assert wait_for_cassandra(), "Cassandra недоступна!"
    
    cluster = Cluster(['cassandra'], port=9042)
    session = cluster.connect()
    
    result = session.execute(
        "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name='wiki_namespace'"
    )
    
    keyspaces = [row.keyspace_name for row in result]
    cluster.shutdown()
    
    assert 'wiki_namespace' in keyspaces


def test_cassandra_table_exists():
    """Перевіряє існування таблиці."""
    cluster = Cluster(['cassandra'], port=9042)
    session = cluster.connect('wiki_namespace')
    
    result = session.execute(
        "SELECT table_name FROM system_schema.tables "
        "WHERE keyspace_name='wiki_namespace' AND table_name='edits'"
    )
    
    tables = [row.table_name for row in result]
    cluster.shutdown()
    
    assert 'edits' in tables


def test_cassandra_table_structure():
    """Перевіряє структуру таблиці."""
    cluster = Cluster(['cassandra'], port=9042)
    session = cluster.connect('wiki_namespace')
    
    result = session.execute(
        "SELECT column_name, type FROM system_schema.columns "
        "WHERE keyspace_name='wiki_namespace' AND table_name='edits'"
    )
    
    columns = {row.column_name: row.type for row in result}
    cluster.shutdown()
    
    required_columns = {
        'id': 'uuid',
        'page_title': 'text',
        'user_text': 'text',
        'dt': 'timestamp'
    }
    
    for col_name, col_type in required_columns.items():
        assert col_name in columns
        assert columns[col_name] == col_type