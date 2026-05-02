"""
Smoke test: Перевірка існування keyspace та таблиці в Cassandra.
Цей тест ПОВИНЕН падати спочатку (RED), поки ми не створимо init.cql.
"""

import pytest
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import time

def test_cassandra_keyspace_exists():
    """
    RED тест: Перевіряє існування keyspace 'wiki_namespace'.
    Очікується помилка, поки не створимо init.cql.
    """
    max_retries = 10
    for attempt in range(max_retries):
        try:
            cluster = Cluster(['cassandra'], port=9042)
            session = cluster.connect()
            
            # Перевірка існування keyspace
            result = session.execute(
                "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name='wiki_namespace'"
            )
            
            keyspaces = [row.keyspace_name for row in result]
            cluster.shutdown()
            
            assert 'wiki_namespace' in keyspaces, "Keyspace 'wiki_namespace' не знайдено!"
            return  # Тест пройшов
            
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
    try:
        cluster = Cluster(['cassandra'], port=9042)
        session = cluster.connect('wiki_namespace')
        
        # Перевірка існування таблиці
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
    RED тест: Перевіряє правильність структури таблиці.
    """
    try:
        cluster = Cluster(['cassandra'], port=9042)
        session = cluster.connect('wiki_namespace')
        
        # Отримуємо опис таблиці
        result = session.execute(
            "SELECT column_name, type FROM system_schema.columns "
            "WHERE keyspace_name='wiki_namespace' AND table_name='edits'"
        )
        
        columns = {row.column_name: row.type for row in result}
        cluster.shutdown()
        
        # Перевіряємо наявність обов'язкових колонок
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