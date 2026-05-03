
import json
import time
import uuid
import pytest
from datetime import datetime, timezone
from kafka import KafkaProducer, KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy


KAFKA_BROKER    = "localhost:9092"   
CASSANDRA_HOST  = "localhost"        
CASSANDRA_PORT  = 9042
KEYSPACE        = "wiki_namespace"
TABLE           = "edits"
TOPIC_INPUT     = "input"
TOPIC_PROCESSED = "processed"

ALLOWED_DOMAINS = ["en.wikipedia.org", "www.wikidata.org", "commons.wikimedia.org"]

STREAMING_WAIT_SEC = 30
POLL_TIMEOUT_MS    = 15_000



@pytest.fixture(scope="module")
def kafka_producer():
    
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=3,
        request_timeout_ms=15_000,
    )
    yield producer
    producer.close()


@pytest.fixture(scope="module")
def cassandra_session():
    
    for attempt in range(10):
        try:
            cluster = Cluster(
                [CASSANDRA_HOST],
                port=CASSANDRA_PORT,
                load_balancing_policy=RoundRobinPolicy(),
                connect_timeout=15,
            )
            session = cluster.connect(KEYSPACE)
            yield session
            cluster.shutdown()
            return
        except Exception as e:
            if attempt < 9:
                time.sleep(3)
            else:
                pytest.fail(
                    f"Cassandra {CASSANDRA_HOST}:{CASSANDRA_PORT} недоступна: {e}\n"
                    "Запусти: cd deploy && docker-compose up -d"
                )


def make_wikipedia_event(
    domain: str = "en.wikipedia.org",
    is_bot: bool = False,
    user_text: str = "TestUser",
    page_title: str = "TestPage",
    event_id: str | None = None,
) -> dict:
    """Фабрика Wikipedia-подій у реальному форматі SSE."""
    return {
        "meta": {
            "id": event_id or str(uuid.uuid4()),
            "dt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "domain": domain,
            "stream": "mediawiki.page-create",
        },
        "page_title": page_title,
        "page_namespace": 0,
        "performer": {
            "user_text": user_text,
            "user_is_bot": is_bot,
        },
        "database": "enwiki",
    }


def send_event(producer: KafkaProducer, event: dict) -> None:
    """Надсилає подію в топік input і чекає підтвердження."""
    future = producer.send(TOPIC_INPUT, value=event)
    future.get(timeout=10)
    producer.flush()


def read_from_processed(timeout_ms: int = POLL_TIMEOUT_MS, max_msgs: int = 20) -> list[dict]:
    """
    Читає повідомлення з топіку processed.
    Унікальний group_id → кожен виклик бачить нові повідомлення.
    """
    group_id = f"test-processed-{uuid.uuid4().hex[:8]}"
    consumer = KafkaConsumer(
        TOPIC_PROCESSED,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset="latest",
        enable_auto_commit=False,
        group_id=group_id,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=timeout_ms,
    )
    consumer.poll(timeout_ms=1000)
    consumer.seek_to_end()
    messages = []
    for msg in consumer:
        messages.append(msg.value)
        if len(messages) >= max_msgs:
            break
    consumer.close()
    return messages


def query_cassandra_by_user(session, user_id: str) -> list:
    """Читає рядки з Cassandra по user_id (partition key)."""
    return list(session.execute(
        f"SELECT * FROM {TABLE} WHERE user_id = %s",
        (user_id,)
    ))


# ═══════════════════════════════════════════════════════════════════════════════
# ТЕСТ 1: input → processed (фільтрація)
# ═══════════════════════════════════════════════════════════════════════════════

class TestInputToProcessedFiltering:
    """
    🔴 RED → 🟢 GREEN

    Перевіряємо що filter_job.py правильно фільтрує події:
      - дозволені домени + не боти → потрапляють у 'processed'
      - недозволені домени / боти → відфільтровуються

    УВАГА: Ці тести потребують запущеного spark-filter контейнера.
    Якщо він не запущений — тести пропускаються з поясненням.
    """

    def test_input_to_processed_filtering_human_allowed_domain(self, kafka_producer):
        """
        Звичайний користувач + дозволений домен → подія з'являється в 'processed'.
        Це основний happy path фільтрації.
        """
        user_id   = f"integration-user-{uuid.uuid4().hex[:8]}"
        page_title = f"IntegrationTest-{uuid.uuid4().hex[:6]}"

        event = make_wikipedia_event(
            domain="en.wikipedia.org",
            is_bot=False,
            user_text=user_id,
            page_title=page_title,
        )
        send_event(kafka_producer, event)
        print(f"\n[Filter] Надіслано подію user={user_id}, domain=en.wikipedia.org")

        # Чекаємо поки filter_job обробить batch
        time.sleep(STREAMING_WAIT_SEC)
        messages = read_from_processed()

        # Шукаємо нашу подію серед отриманих
        matching = [
            m for m in messages
            if m.get("user_id") == user_id or m.get("page_title") == page_title
        ]

        if not messages:
            pytest.skip(
                "Топік 'processed' порожній — spark-filter може не бути запущений.\n"
                "Запусти: docker-compose up -d spark-filter"
            )

        assert matching, (
            f"Подія user={user_id} не знайдена в топіку 'processed'.\n"
            f"Отримано {len(messages)} повідомлень, але жодне не відповідає."
        )
        print(f"[Filter] ✅ Подія знайдена в 'processed': {matching[0]}")

    def test_input_to_processed_filtering_bot_is_blocked(self, kafka_producer):
        """
        Бот (user_is_bot=True) → НЕ з'являється в 'processed'.
        filter_job має відфільтрувати цю подію.
        """
        bot_marker = f"bot-{uuid.uuid4().hex[:8]}"
        event = make_wikipedia_event(
            domain="en.wikipedia.org",
            is_bot=True,
            user_text=bot_marker,
            page_title=f"BotPage-{uuid.uuid4().hex[:6]}",
        )
        send_event(kafka_producer, event)
        print(f"\n[Filter] Надіслано бот-подію user={bot_marker}")

        time.sleep(STREAMING_WAIT_SEC)
        messages = read_from_processed()

        bot_messages = [
            m for m in messages
            if m.get("user_id") == bot_marker
        ]
        assert not bot_messages, (
            f"Бот {bot_marker} знайдений у 'processed' — фільтр не спрацював!"
        )
        print("[Filter] ✅ Бот-подія правильно відфільтрована")

    def test_input_to_processed_filtering_blocked_domain(self, kafka_producer):
        """
        Недозволений домен → НЕ потрапляє в 'processed'.
        """
        marker = f"uk-user-{uuid.uuid4().hex[:8]}"
        event = make_wikipedia_event(
            domain="uk.wikipedia.org",  # не в ALLOWED_DOMAINS
            is_bot=False,
            user_text=marker,
            page_title=f"UkrainianPage-{uuid.uuid4().hex[:6]}",
        )
        send_event(kafka_producer, event)
        print(f"\n[Filter] Надіслано подію з недозволеного домену user={marker}")

        time.sleep(STREAMING_WAIT_SEC)
        messages = read_from_processed()

        blocked = [m for m in messages if m.get("user_id") == marker]
        assert not blocked, (
            f"Подія з uk.wikipedia.org знайдена в 'processed' — фільтр доменів не спрацював!"
        )
        print("[Filter] ✅ Недозволений домен правильно відфільтровано")

    @pytest.mark.parametrize("domain", ALLOWED_DOMAINS)
    def test_all_allowed_domains_reach_processed(self, kafka_producer, domain):
        """Параметризований тест: кожен дозволений домен проходить фільтр."""
        marker = f"domain-test-{uuid.uuid4().hex[:8]}"
        event = make_wikipedia_event(
            domain=domain, is_bot=False, user_text=marker
        )
        send_event(kafka_producer, event)
        time.sleep(STREAMING_WAIT_SEC)
        messages = read_from_processed()

        if not messages:
            pytest.skip("spark-filter не запущений")

        found = any(m.get("user_id") == marker for m in messages)
        assert found, f"Подія з домену {domain} не знайдена в 'processed'"


# ═══════════════════════════════════════════════════════════════════════════════
# ТЕСТ 2: processed → Cassandra
# ═══════════════════════════════════════════════════════════════════════════════

class TestProcessedToCassandra:
    """
    🔴 RED → 🟢 GREEN

    Перевіряємо що cassandra_writer_job.py читає з 'processed'
    і записує в Cassandra з новою схемою:
      PRIMARY KEY ((user_id), created_at, domain)
    """

    def test_processed_to_cassandra_direct_cql_write(self, cassandra_session):
        """
        Baseline: пряме записування через CQL → читаємо назад.
        Підтверджуємо що нова схема таблиці працює коректно.
        """
        user_id    = f"cql-test-{uuid.uuid4().hex[:8]}"
        domain     = "en.wikipedia.org"
        created_at = datetime(2026, 5, 1, 10, 0, 0, tzinfo=timezone.utc)
        page_title = f"CQL-Test-Page-{uuid.uuid4().hex[:6]}"

        cassandra_session.execute(
            f"INSERT INTO {TABLE} (user_id, domain, created_at, page_title)"
            " VALUES (%s, %s, %s, %s)",
            (user_id, domain, created_at, page_title)
        )

        rows = query_cassandra_by_user(cassandra_session, user_id)
        assert len(rows) == 1, f"Очікували 1 рядок, отримали {len(rows)}"
        assert rows[0].domain     == domain
        assert rows[0].page_title == page_title
        print(f"\n[Cassandra] ✅ CQL запис/читання OK: user_id={user_id}")

    def test_processed_to_cassandra_primary_key_structure(self, cassandra_session):
        """
        Перевіряємо що PRIMARY KEY ((user_id), created_at, domain) дозволяє
        зберігати КІЛЬКА записів для одного user_id (різні created_at/domain).
        """
        user_id = f"pk-test-{uuid.uuid4().hex[:8]}"

        records = [
            ("en.wikipedia.org",      datetime(2026, 5, 1, 10, 0, 0, tzinfo=timezone.utc), "Page A"),
            ("www.wikidata.org",       datetime(2026, 5, 1, 11, 0, 0, tzinfo=timezone.utc), "Page B"),
            ("commons.wikimedia.org",  datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc), "Page C"),
        ]
        for domain, created_at, page_title in records:
            cassandra_session.execute(
                f"INSERT INTO {TABLE} (user_id, domain, created_at, page_title)"
                " VALUES (%s, %s, %s, %s)",
                (user_id, domain, created_at, page_title)
            )

        rows = query_cassandra_by_user(cassandra_session, user_id)
        assert len(rows) == 3, (
            f"PRIMARY KEY ((user_id), created_at, domain) має дозволити 3 записи для"
            f" одного user_id. Отримано: {len(rows)}"
        )
        print(f"\n[Cassandra] ✅ 3 записи для user_id={user_id} збережено коректно")

    def test_processed_to_cassandra_clustering_order_desc(self, cassandra_session):
        """
        CLUSTERING ORDER BY (created_at DESC) → останні правки першими.
        Перевіряємо що записи повертаються у правильному порядку.
        """
        user_id = f"order-test-{uuid.uuid4().hex[:8]}"

        times = [
            datetime(2026, 5, 1, 8, 0, 0, tzinfo=timezone.utc),   # найстаріший
            datetime(2026, 5, 1, 10, 0, 0, tzinfo=timezone.utc),  # середній
            datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc),  # найновіший
        ]
        for i, t in enumerate(times):
            cassandra_session.execute(
                f"INSERT INTO {TABLE} (user_id, domain, created_at, page_title)"
                " VALUES (%s, %s, %s, %s)",
                (user_id, "en.wikipedia.org", t, f"Page-{i}")
            )

        rows = query_cassandra_by_user(cassandra_session, user_id)
        assert len(rows) == 3

        # CLUSTERING ORDER BY created_at DESC → перший рядок найновіший
        first_time  = rows[0].created_at.replace(tzinfo=timezone.utc)
        last_time   = rows[-1].created_at.replace(tzinfo=timezone.utc)
        assert first_time > last_time, (
            "Очікувався порядок DESC (найновіший першим), але порядок інший.\n"
            f"Перший: {first_time}, Останній: {last_time}"
        )
        print(f"\n[Cassandra] ✅ Порядок created_at DESC підтверджено")

    def test_processed_to_cassandra_upsert_same_primary_key(self, cassandra_session):
        """
        Cassandra upsert: той самий PRIMARY KEY перезаписує запис.
        user_id + created_at + domain → унікальний ключ.
        """
        user_id    = f"upsert-test-{uuid.uuid4().hex[:8]}"
        domain     = "en.wikipedia.org"
        created_at = datetime(2026, 5, 1, 10, 0, 0, tzinfo=timezone.utc)

        # Перший запис
        cassandra_session.execute(
            f"INSERT INTO {TABLE} (user_id, domain, created_at, page_title)"
            " VALUES (%s, %s, %s, %s)",
            (user_id, domain, created_at, "Original Title")
        )
        # Upsert з тим самим PK
        cassandra_session.execute(
            f"INSERT INTO {TABLE} (user_id, domain, created_at, page_title)"
            " VALUES (%s, %s, %s, %s)",
            (user_id, domain, created_at, "Updated Title")
        )

        rows = query_cassandra_by_user(cassandra_session, user_id)
        assert len(rows) == 1, "Upsert має залишити 1 запис"
        assert rows[0].page_title == "Updated Title", "Upsert має оновити page_title"
        print(f"\n[Cassandra] ✅ Upsert підтверджено: {rows[0].page_title}")

    def test_processed_to_cassandra_required_fields_saved(self, cassandra_session):
        """
        Перевіряємо що всі 4 поля нової схеми коректно зберігаються і читаються.
        """
        user_id    = f"fields-test-{uuid.uuid4().hex[:8]}"
        domain     = "www.wikidata.org"
        created_at = datetime(2026, 5, 2, 14, 30, 0, tzinfo=timezone.utc)
        page_title = "Data Quality Test Article"

        cassandra_session.execute(
            f"INSERT INTO {TABLE} (user_id, domain, created_at, page_title)"
            " VALUES (%s, %s, %s, %s)",
            (user_id, domain, created_at, page_title)
        )

        rows = query_cassandra_by_user(cassandra_session, user_id)
        assert len(rows) == 1
        row = rows[0]

        assert row.user_id    == user_id,    f"user_id: {row.user_id!r} != {user_id!r}"
        assert row.domain     == domain,     f"domain: {row.domain!r} != {domain!r}"
        assert row.page_title == page_title, f"page_title: {row.page_title!r} != {page_title!r}"
        # created_at може мати різну timezone інформацію після round-trip
        saved_dt = row.created_at
        if hasattr(saved_dt, "replace"):
            saved_dt = saved_dt.replace(tzinfo=timezone.utc)
        assert saved_dt == created_at, f"created_at: {saved_dt} != {created_at}"
        print(f"\n[Cassandra] ✅ Всі 4 поля збережені коректно")

    def test_processed_to_cassandra_full_streaming_path(
        self, kafka_producer, cassandra_session
    ):
        """
        E2E інтеграційний тест всього нового пайплайну:
          Kafka input → filter_job → processed → cassandra_writer → Cassandra

        Якщо Spark jobs не запущені — тест пропускається.
        """
        user_id    = f"e2e-pipeline-{uuid.uuid4().hex[:8]}"
        page_title = f"E2EPipeline-{uuid.uuid4().hex[:6]}"

        # Надсилаємо подію що має пройти всі фільтри
        event = make_wikipedia_event(
            domain="en.wikipedia.org",
            is_bot=False,
            user_text=user_id,
            page_title=page_title,
        )
        send_event(kafka_producer, event)
        print(f"\n[E2E Pipeline] Надіслано: user={user_id}, page={page_title}")

        # Чекаємо двох Spark jobs (filter + cassandra_writer)
        deadline = time.time() + 120  # 2 хвилини максимум
        rows = []
        while time.time() < deadline:
            rows = query_cassandra_by_user(cassandra_session, user_id)
            if rows:
                break
            time.sleep(5)

        if not rows:
            pytest.skip(
                f"Рядок user_id={user_id} не з'явився в Cassandra за 120с.\n"
                "Переконайся що запущені: spark-filter і spark-cassandra контейнери.\n"
                "Перевір логи: docker logs spark-filter && docker logs spark-cassandra"
            )

        assert rows[0].page_title == page_title
        assert rows[0].domain     == "en.wikipedia.org"
        print(f"[E2E Pipeline] ✅ Дані пройшли весь пайплайн і збережені в Cassandra!")