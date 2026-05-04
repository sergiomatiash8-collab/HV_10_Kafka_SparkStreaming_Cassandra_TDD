import pytest
from datetime import datetime, timezone
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy
import time

CASSANDRA_HOST = '127.0.0.1'
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
    assert wait_for_cassandra(), (
        f'Cassandra {CASSANDRA_HOST}:{CASSANDRA_PORT} not respond!\n'
        'Set it up: cd deploy && docker-compose up -d'
    )
    
    c = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT,
                load_balancing_policy=RoundRobinPolicy())
    s = c.connect()
    rows = list(s.execute(
        "SELECT keyspace_name FROM system_schema.keyspaces"
        " WHERE keyspace_name='wiki_namespace'"
    ))
    c.shutdown()
    assert rows, 'Keyspace wiki_namespace not exists!'

def test_write_to_cassandra():
    assert wait_for_cassandra(), 'Cassandra недоступна!'

    c = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT,
                load_balancing_policy=RoundRobinPolicy())
    s = c.connect('wiki_namespace')

    # Дані для тесту (відповідно до PRIMARY KEY: domain, created_at, user_id)
    test_user_id = 'user_12345'
    test_domain = 'uk.wikipedia.org'
    test_dt = datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)
    test_title = 'Smoke Test Page'
    test_is_bot = False

    try:
        # Видаляємо старі дані тесту, якщо вони є (щоб тест був чистим)
        s.execute(
            'DELETE FROM edits WHERE domain=%s AND created_at=%s AND user_id=%s',
            (test_domain, test_dt, test_user_id)
        )

        # Запис (використовуємо актуальні назви колонок)
        s.execute(
            '''
            INSERT INTO edits (user_id, domain, created_at, page_title, user_is_bot)
            VALUES (%s, %s, %s, %s, %s)
            ''',
            (test_user_id, test_domain, test_dt, test_title, test_is_bot)
        )
        print('\n[SUCCESS] Writing is successful!')

        # Читання та повна перевірка всіх полів
        query = 'SELECT * FROM edits WHERE domain=%s AND created_at=%s AND user_id=%s'
        rows = list(s.execute(query, (test_domain, test_dt, test_user_id)))
        
        assert len(rows) == 1, 'Data not found after being written!'
        res = rows[0]
        
        assert res.user_id == test_user_id
        assert res.domain == test_domain
        assert res.page_title == test_title
        assert res.user_is_bot == test_is_bot
        # Cassandra повертає naive datetime в UTC, порівнюємо значення
        assert res.created_at.replace(tzinfo=timezone.utc) == test_dt
        
        print(f'[SUCCESS] Verification passed for title: {res.page_title}')

    finally:
        c.shutdown()