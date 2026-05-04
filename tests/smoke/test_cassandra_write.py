"""
Smoke test: Writing to Cassandra.

IMPORTANT: This test uses the cassandra-driver (not Spark) for writing,
as PySpark 3.5.0 is not compatible with Python 3.12+.
The Spark connector is tested separately in test_cassandra_connection.py.
"""
import uuid
import pytest
from datetime import datetime, timezone
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy
import time

CASSANDRA_HOST = 'localhost'   
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
    assert rows, (
        'Keyspace wiki_namespace not exists!\n'
        'Set it up: docker exec cassandra cqlsh -f /init.cql'
    )


def test_write_to_cassandra():
    
    assert wait_for_cassandra(), 'Cassandra недоступна!'

    c = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT,
                load_balancing_policy=RoundRobinPolicy())
    s = c.connect('wiki_namespace')

    test_id = uuid.UUID('550e8400-e29b-41d4-a716-446655440099')
    test_dt = datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)

    try:
        
        s.execute(
            'INSERT INTO wiki_namespace.edits (id, page_title, user_text, dt)'
            ' VALUES (%s, %s, %s, %s)',
            (test_id, 'Smoke Test Page', 'SmokeTestUser', test_dt)
        )
        print('Writing is successful!')

        
        rows = list(s.execute(
            'SELECT * FROM wiki_namespace.edits WHERE id = %s',
            (test_id,)
        ))
        assert len(rows) == 1, 'Not found after being written!'
        assert rows[0].page_title == 'Smoke Test Page'
        assert rows[0].user_text  == 'SmokeTestUser'
        print(f'Reading is successful: {rows[0].page_title}')

    finally:
        c.shutdown()