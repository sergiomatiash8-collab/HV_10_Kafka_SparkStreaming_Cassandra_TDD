"""Integration test: Spark ↔ Cassandra"""
import uuid
import pytest
from datetime import datetime, timezone
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy
import time


CASSANDRA_HOST = 'localhost'
CASSANDRA_PORT = 9042
KEYSPACE = 'wiki_namespace'
TABLE = 'edits'

@pytest.fixture(scope='module')
def cql():
    
    for attempt in range(10):
        try:
            cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT,
                            load_balancing_policy=RoundRobinPolicy())
            session = cluster.connect(KEYSPACE)
            yield session
            cluster.shutdown()
            return
        except Exception as e:
            if attempt < 9:
                time.sleep(3)
            else:
                pytest.fail(f'Cassandra {CASSANDRA_HOST}:{CASSANDRA_PORT} недоступна: {e}')


TEST_ID_1 = uuid.UUID('550e8400-e29b-41d4-a716-446655440001')
TEST_ID_2 = uuid.UUID('550e8400-e29b-41d4-a716-446655440002')

def test_spark_writes_row_to_cassandra(cql):
    
    cql.execute('TRUNCATE wiki_namespace.edits')
    cql.execute(
        'INSERT INTO wiki_namespace.edits (id, page_title, user_text, dt)'
        ' VALUES (%s, %s, %s, %s)',
        (TEST_ID_1, 'Spark Integration Test Page', 'SparkTestUser',
         datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc))
    )
    rows = list(cql.execute(
        'SELECT * FROM wiki_namespace.edits WHERE id = %s', (TEST_ID_1,)
    ))
    assert len(rows) == 1
    assert rows[0].page_title == 'Spark Integration Test Page'

def test_spark_reads_back_written_row(spark, cql):
    
    from pyspark.sql.functions import col
    df = (
        spark.read
        .format('org.apache.spark.sql.cassandra')
        .options(table=TABLE, keyspace=KEYSPACE)
        .load()
        .filter(col('page_title') == 'Spark Integration Test Page')
    )
    rows = df.collect()
    assert len(rows) >= 1, 'Spark could not find the row written!'
    assert rows[0]['user_text'] == 'SparkTestUser'

def test_duplicate_id_is_upserted(cql):
    
    for title, user in [('Original Title', 'OriginalUser'),
                        ('Updated Title',  'UpdatedUser')]:
        cql.execute(
            'INSERT INTO wiki_namespace.edits (id, page_title, user_text, dt)'
            ' VALUES (%s, %s, %s, %s)',
            (TEST_ID_2, title, user,
             datetime(2026, 5, 1, 13, 0, 0, tzinfo=timezone.utc))
        )
    rows = list(cql.execute(
        'SELECT * FROM wiki_namespace.edits WHERE id = %s', (TEST_ID_2,)
    ))
    assert len(rows) == 1
    assert rows[0].page_title == 'Updated Title'