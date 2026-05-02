# tests/smoke/test_cassandra_write.py
"""
Smoke test: Запис в Cassandra.

ВАЖЛИВО: Тест використовує cassandra-driver (не Spark) для запису,
бо PySpark 3.5.0 несумісний з Python 3.12+.
Spark connector перевіряється окремо в test_cassandra_connection.py.
"""
import uuid
import pytest
from datetime import datetime, timezone
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy
import time

CASSANDRA_HOST = 'localhost'   # ВИПРАВЛЕННЯ: не 'cassandra'
CASSANDRA_PORT = 9042


def wait_for_cassandra(retries=12, delay=5):
    for i in range(retries):
        try:
            c = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT,
                        load_balancing_policy=RoundRobinPolicy())
            s = c.connect()
            c.shutdown()
            return True
        except Exception:
            time.sleep(delay)
    return False


def test_cassandra_setup():
    """Перевіряє що Cassandra ready і схема ініціалізована."""
    assert wait_for_cassandra(), (
        f'Cassandra {CASSANDRA_HOST}:{CASSANDRA_PORT} не відповідає!\n'
        'Запусти: cd deploy && docker-compose up -d'
    )
    # Перевіряємо що схема ініціалізована
    c = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT,
                load_balancing_policy=RoundRobinPolicy())
    s = c.connect()
    rows = list(s.execute(
        "SELECT keyspace_name FROM system_schema.keyspaces"
        " WHERE keyspace_name='wiki_namespace'"
    ))
    c.shutdown()
    assert rows, (
        'Keyspace wiki_namespace не існує!\n'
        'Запусти: docker exec cassandra cqlsh -f /init.cql'
    )


def test_write_to_cassandra():
    """
    Записує рядок через cassandra-driver і читає назад.

    Використовуємо cassandra-driver (не Spark) бо:
    - PySpark 3.5.0 не підтримує Python 3.12+
    - cassandra-driver сумісний з будь-якою версією Python
    - Smoke-тест перевіряє підключення, не Spark-специфіку
    """
    assert wait_for_cassandra(), 'Cassandra недоступна!'

    c = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT,
                load_balancing_policy=RoundRobinPolicy())
    s = c.connect('wiki_namespace')

    test_id = uuid.UUID('550e8400-e29b-41d4-a716-446655440099')
    test_dt = datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)

    try:
        # Запис через CQL — надійно на будь-якому Python
        s.execute(
            'INSERT INTO wiki_namespace.edits (id, page_title, user_text, dt)'
            ' VALUES (%s, %s, %s, %s)',
            (test_id, 'Smoke Test Page', 'SmokeTestUser', test_dt)
        )
        print('Запис успішний!')

        # Читання назад
        rows = list(s.execute(
            'SELECT * FROM wiki_namespace.edits WHERE id = %s',
            (test_id,)
        ))
        assert len(rows) == 1, 'Рядок не знайдено після запису!'
        assert rows[0].page_title == 'Smoke Test Page'
        assert rows[0].user_text  == 'SmokeTestUser'
        print(f'Читання успішне: {rows[0].page_title}')

    finally:
        c.shutdown()