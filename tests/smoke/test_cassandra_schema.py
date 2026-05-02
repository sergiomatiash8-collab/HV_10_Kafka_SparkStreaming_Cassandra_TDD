"""
Smoke test: Перевірка існування keyspace та таблиці в Cassandra.
"""

import pytest
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import time

# Конфігурація підключення
CASSANDRA_HOSTS = ['127.0.0.1']  # Виправлено для роботи зовні Docker
CASSANDRA_PORT = 9042
# Якщо в Docker-compose не вказано пароль, залиш порожніми або видали auth_provider
CASSANDRA_USER = 'cassandra'
CASSANDRA_PASS = 'cassandra'

def test_cassandra_keyspace_exists():
    """
    RED тест: Перевіряє існування keyspace 'wiki_namespace'.
    """
    auth_provider = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASS)
    max_retries = 10
    
    for attempt in range(max_retries):
        try:
            cluster = Cluster(CASSANDRA_HOSTS, port=CASSANDRA_PORT, auth_provider=auth_provider)
            session = cluster.connect()
            
            result = session.execute(
                "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name='wiki_namespace'"
            )
            
            keyspaces = [row.keyspace_name for row in result]
            cluster.shutdown()
            
            assert 'wiki_namespace' in keyspaces, "Keyspace 'wiki_namespace' не знайдено!"
            return 
            
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Спроба {attempt + 1}/{max_retries} не вдалася: {e}")
                time.sleep(5)
            else:
                pytest.fail(f"Cassandra недоступна після {max_retries} спроб: {e}")

def test_cassandra_table_exists():
    """
    RED тест: Перевіряє існування таблиці 'edits'.
    """
    auth_provider = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASS)
    try:
        cluster = Cluster(CASSANDRA_HOSTS, port=CASSANDRA_PORT, auth_provider=auth_provider)
        session = cluster.connect('wiki_namespace')
        
        result = session.execute(
            "SELECT table_name FROM system_schema.tables "
            "WHERE keyspace_name='wiki_namespace' AND table_name='edits'"
        )
        
        tables = [row.table_name for row in result]
        cluster.shutdown()
        
        assert 'edits' in tables, "Таблиця 'edits' не знайдена!"
        
    except Exception as e:
        pytest.fail(f"Помилка перевірки таблиці: {e}")

def test_cassandra_table_structure():
    """
    RED тест: Перевіряє структуру таблиці.
    """
    auth_provider = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASS)
    try:
        cluster = Cluster(CASSANDRA_HOSTS, port=CASSANDRA_PORT, auth_provider=auth_provider)
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
            assert col_name in columns, f"Колонка '{col_name}' відсутня!"
            assert columns[col_name] == col_type, \
                f"Колонка '{col_name}' має тип '{columns[col_name]}', очікувалося '{col_type}'"
        
    except Exception as e:
        pytest.fail(f"Помилка перевірки структури: {e}")