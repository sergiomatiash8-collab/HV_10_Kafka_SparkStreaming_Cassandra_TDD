# tests/smoke/test_cassandra_schema.py
"""Smoke test: Перевірка схеми Cassandra."""
import pytest
from cassandra.cluster import Cluster
import time

# ВИПРАВЛЕННЯ: localhost замість 'cassandra' (Docker hostname)
CASSANDRA_HOST = 'localhost'
CASSANDRA_PORT = 9042


def get_session(keyspace=None):
    """Допоміжна функція підключення з retry."""
    for attempt in range(10):
        try:
            cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
            session = cluster.connect(keyspace) if keyspace else cluster.connect()
            return cluster, session
        except Exception:
            if attempt < 9:
                time.sleep(3)
    pytest.fail(f'Cassandra {CASSANDRA_HOST}:{CASSANDRA_PORT} недоступна!')


def test_cassandra_keyspace_exists():
    """Перевіряє існування keyspace wiki_namespace."""
    cluster, session = get_session()
    try:
        result = session.execute(
            "SELECT keyspace_name FROM system_schema.keyspaces"
            " WHERE keyspace_name='wiki_namespace'"
        )
        keyspaces = [r.keyspace_name for r in result]
        assert 'wiki_namespace' in keyspaces, (
            'Keyspace wiki_namespace не існує!\n'
            'Запусти: docker exec cassandra cqlsh -f /init.cql'
        )
    finally:
        cluster.shutdown()


def test_cassandra_table_exists():
    """Перевіряє існування таблиці edits."""
    cluster, session = get_session('wiki_namespace')
    try:
        result = session.execute(
            "SELECT table_name FROM system_schema.tables"
            " WHERE keyspace_name='wiki_namespace' AND table_name='edits'"
        )
        tables = [r.table_name for r in result]
        assert 'edits' in tables, 'Таблиця edits не існує!'
    finally:
        cluster.shutdown()


def test_cassandra_table_structure():
    """Перевіряє структуру таблиці: поля та типи."""
    cluster, session = get_session('wiki_namespace')
    try:
        result = session.execute(
            "SELECT column_name, type FROM system_schema.columns"
            " WHERE keyspace_name='wiki_namespace' AND table_name='edits'"
        )
        columns = {r.column_name: r.type for r in result}
        assert columns, 'Таблиця edits порожня або не існує!'
        required = {'id': 'uuid', 'page_title': 'text',
                    'user_text': 'text', 'dt': 'timestamp'}
        for col_name, col_type in required.items():
            assert col_name in columns, f'Відсутнє поле: {col_name}'
            assert columns[col_name] == col_type, (
                f'Поле {col_name}: очікується {col_type}, є {columns[col_name]}'
            )
    finally:
        cluster.shutdown()