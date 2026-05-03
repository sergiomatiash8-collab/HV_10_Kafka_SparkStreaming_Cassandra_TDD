import json
import time
import uuid
import pytest
from kafka import KafkaProducer
from cassandra.cluster import Cluster

# Configuration
KAFKA_BROKER = 'localhost:9092'
CASSANDRA_HOST = 'localhost'
KEYSPACE = 'wiki_namespace'
TABLE = 'edits'
TOPIC = 'input'
MAX_WAIT_SECONDS = 60  # Spark Streaming needs time
POLL_INTERVAL = 5

@pytest.fixture(scope='module')
def cassandra_session():
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect(KEYSPACE)
    yield session
    cluster.shutdown()

@pytest.fixture(scope='module')
def kafka_producer():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        retries=3
    )
    yield producer
    producer.close()

def send_synthetic_wikipedia_event(producer, event_id, title):
    """Creating event in format of real Wikipedia SSE-stream."""
    event = {
        'meta': {
            'id': event_id,
            'dt': '2026-05-01T10:00:00.000Z',
            'domain': 'en.wikipedia.org',
            'stream': 'mediawiki.page-create'
        },
        'page_title': title,
        'page_namespace': 0,
        'performer': {
            'user_text': 'e2e-test-bot',
            'user_is_bot': True
        },
        'database': 'enwiki'
    }
    producer.send(TOPIC, value=event)
    producer.flush()

def wait_for_cassandra_row(session, event_id, timeout=MAX_WAIT_SECONDS):
    """Pulling Cassandra before the row appears."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        query = f"SELECT * FROM {TABLE} WHERE id = {event_id}"
        rows = list(session.execute(query))
        if rows:
            return rows[0]
        time.sleep(POLL_INTERVAL)
    return None

def test_e2e_event_appears_in_cassandra(kafka_producer, cassandra_session):
    """Main E2E test: Event trough Kafka & Spark ends in Cassandra."""
    event_id = str(uuid.uuid4())
    title = f'E2E-Test-{event_id[:8]}'

    
    send_synthetic_wikipedia_event(kafka_producer, event_id, title)
    print(f'\n[E2E] Event sent id={event_id}, title={title}')

    
    row = wait_for_cassandra_row(cassandra_session, event_id)

    
    assert row is not None, (
        f"Row with id={event_id} not appeared in Cassandra during {MAX_WAIT_SECONDS}с. "
        "Check logs in Spark: docker logs spark-processor"
    )
    
    assert row.page_title == title
    assert row.user_text == 'e2e-test-bot'
    print(f'[E2E] Row successfully ended in Cassandra!')

def test_e2e_multiple_events_all_persisted(kafka_producer, cassandra_session):
    """Stability check: sending bathc of events."""
    count = 3
    event_ids = [str(uuid.uuid4()) for _ in range(count)]
    
    for eid in event_ids:
        send_synthetic_wikipedia_event(kafka_producer, eid, f'Batch-Test-{eid[:8]}')
    
    print(f'\n[E2E] Sending batch of {count} events')
    
    
    time.sleep(POLL_INTERVAL * 2)
    
    missing = []
    for eid in event_ids:
        if not wait_for_cassandra_row(cassandra_session, eid, timeout=20):
            missing.append(eid)
            
    assert not missing, f"Some events are missed: {missing}"
    print(f'[E2E] All {count} batch events saved.')