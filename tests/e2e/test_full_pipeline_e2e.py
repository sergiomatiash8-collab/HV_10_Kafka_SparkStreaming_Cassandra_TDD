import json, time, uuid, pytest
from datetime import datetime, timezone
from kafka import KafkaProducer
from cassandra.cluster import Cluster


KAFKA_BROKER = "localhost:9092"
CASSANDRA_HOST = "localhost"
KEYSPACE, TABLE = "wiki_namespace", "edits"
TOPIC_INPUT = "input"

@pytest.fixture(scope="module")
def kafka_producer():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    yield producer
    producer.close()

@pytest.fixture(scope="module")
def cassandra_session():
    cluster = Cluster([CASSANDRA_HOST], port=9042, connect_timeout=10)
    try:
        session = cluster.connect(KEYSPACE)
        yield session
    finally:
        cluster.shutdown()

def make_event(user="User", domain="en.wikipedia.org", is_bot=False):
    return {
        "meta": {
            "id": str(uuid.uuid4()),
            "dt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "domain": domain,
        },
        "page_title": "Test Page",
        "performer": {"user_text": user, "user_is_bot": is_bot}
    }

def wait_for_data(func, timeout=60, step=5):
    start = time.time()
    while time.time() - start < timeout:
        res = func()
        if res: return res
        time.sleep(step)
    return None

class TestE2EPipeline:
   

    def test_full_flow_success(self, kafka_producer, cassandra_session):
        user_id = f"e2e_{uuid.uuid4().hex[:6]}"
        event = make_event(user=user_id)
        
        kafka_producer.send(TOPIC_INPUT, event).get(timeout=10)
        print(f"\n[E2E] Sent for {user_id}")

        
        query = f"SELECT * FROM {TABLE} WHERE user_id = %s ALLOW FILTERING"
        row = wait_for_data(lambda: cassandra_session.execute(query, (user_id,)).one())

        assert row, f"Data for {user_id} not found!"
        print(f"[E2E]  Found in DB: {row.user_id}")

    @pytest.mark.parametrize("domain, is_bot, should_pass", [
        ("en.wikipedia.org", True, False),
        ("uk.wikipedia.org", False, False),
        ("www.wikidata.org", False, True),
    ])
    def test_filtering_logic(self, kafka_producer, cassandra_session, domain, is_bot, should_pass):
        user_id = f"filter_{uuid.uuid4().hex[:6]}"
        event = make_event(user=user_id, domain=domain, is_bot=is_bot)
        
        kafka_producer.send(TOPIC_INPUT, event).get(timeout=10)
        time.sleep(15) 
        
        
        query = f"SELECT * FROM {TABLE} WHERE user_id=%s ALLOW FILTERING"
        row = cassandra_session.execute(query, (user_id,)).one()
        
        if should_pass:
            assert row, f"Valid event {user_id} filtered out!"
        else:
            assert not row, f"Invalid event {user_id} leaked!"

    def test_upsert_consistency(self, cassandra_session):
        uid, dom = f"upsert_{uuid.uuid4().hex[:4]}", "en.wikipedia.org"
        ts = datetime(2026, 5, 4, 12, 0, 0, tzinfo=timezone.utc)
        
        for title in ["First", "Second"]:
            cassandra_session.execute(
                f"INSERT INTO {TABLE} (user_id, domain, created_at, page_title) VALUES (%s,%s,%s,%s)",
                (uid, dom, ts, title)
            )
        
        
        query = f"SELECT * FROM {TABLE} WHERE user_id=%s ALLOW FILTERING"
        rows = list(cassandra_session.execute(query, (uid,)))
        assert len(rows) == 1
        assert rows[0].page_title == "Second"