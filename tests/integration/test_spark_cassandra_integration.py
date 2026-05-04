import pytest
from datetime import datetime, timezone
from cassandra.cluster import Cluster
import time

CASSANDRA_HOST = '127.0.0.1'
KEYSPACE = 'wiki_namespace'
TABLE = 'edits'

@pytest.fixture(scope='module')
def cql():
    
    for attempt in range(10):
        try:
            cluster = Cluster([CASSANDRA_HOST])
            session = cluster.connect()
            session.execute(f"CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} WITH replication = {{'class':'SimpleStrategy', 'replication_factor':1}};")
            
            session.execute(f"""
                CREATE TABLE IF NOT EXISTS {KEYSPACE}.{TABLE} (
                    domain text,
                    created_at timestamp,
                    user_id text,
                    page_title text,
                    PRIMARY KEY (domain, created_at, user_id)
                );
            """)
            session.set_keyspace(KEYSPACE)
            yield session
            cluster.shutdown()
            return
        except Exception as e:
            if attempt < 9:
                time.sleep(2)
            else:
                pytest.fail(f"Cassandra недоступна: {e}")

def test_spark_writes_and_reads_cassandra(spark, cql):
    
    
    cql.execute(f'TRUNCATE {TABLE}')
    
    
    data = [('en.wikipedia.org', datetime(2026, 5, 4, 13, 0, 0, tzinfo=timezone.utc), 'user_123', 'Main Page')]
    df = spark.createDataFrame(data, ["domain", "created_at", "user_id", "page_title"])
    
    
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=TABLE, keyspace=KEYSPACE) \
        .mode("append") \
        .save()
    
    
    read_df = spark.read \
        .format('org.apache.spark.sql.cassandra') \
        .options(table=TABLE, keyspace=KEYSPACE) \
        .load() \
        .filter("user_id = 'user_123'")
    
    rows = read_df.collect()
    
    assert len(rows) == 1
    assert rows[0]['domain'] == 'en.wikipedia.org'
    assert rows[0]['page_title'] == 'Main Page'

def test_duplicate_id_is_upserted(cql):
    
    now = datetime.now(timezone.utc)
    for title in ['Original Title', 'Updated Title']:
        cql.execute(
            f'INSERT INTO {TABLE} (domain, created_at, user_id, page_title) VALUES (%s, %s, %s, %s)',
            ('uk.wikipedia.org', now, 'user_456', title)
        )
    
    rows = list(cql.execute(f"SELECT page_title FROM {TABLE} WHERE domain='uk.wikipedia.org' AND created_at=%s AND user_id='user_456'", (now,)))
    assert len(rows) == 1
    assert rows[0].page_title == 'Updated Title'