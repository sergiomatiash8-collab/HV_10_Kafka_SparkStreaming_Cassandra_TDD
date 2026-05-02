import json
import uuid
import pytest
import time
from kafka import KafkaProducer, KafkaConsumer
from src.spark.logic import transform_wikipedia_event

KAFKA_BROKER = 'localhost:9092'
TOPIC = 'input'

@pytest.fixture(scope='module')
def kafka_producer():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',  # Чекаємо підтвердження від усіх реплік
        retries=3,
    )
    yield producer
    producer.close()

@pytest.fixture(scope='module')
def kafka_consumer():
    # Використовуємо новий group_id для кожного запуску, щоб читати з чистим офсетом
    group_id = f'test-group-{uuid.uuid4().hex}'
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='latest',  # Тільки нові повідомлення
        group_id=group_id,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=2000,   # Не чекати вічно, якщо даних немає
    )
    
    # "Прогрів" підключення
    for _ in range(10):
        consumer.poll(timeout_ms=500)
        if consumer.assignment():
            break
        time.sleep(0.5)
        
    yield consumer
    consumer.close()

def test_kafka_producer_can_send_message(kafka_producer, kafka_consumer):
    """Бронебійна перевірка: відправляємо і шукаємо ID серед будь-якого трафіку."""
    unique_msg_id = str(uuid.uuid4())
    
    test_msg = {
        'meta': {'id': unique_msg_id, 'dt': '2026-05-01T10:00:00Z'},
        'page_title': f'Test Page {unique_msg_id}',
        'performer': {'user_text': 'TestUser'}
    }

    # Відправляємо і ЖОРСТКО чекаємо запису в Kafka
    kafka_producer.send(TOPIC, value=test_msg).get(timeout=10)
    kafka_producer.flush()

    found_msg = None
    start_time = time.time()
    timeout_limit = 15  # 15 секунд на пошук

    # Цикл пошуку
    while time.time() - start_time < timeout_limit:
        records = kafka_consumer.poll(timeout_ms=1000)
        
        for tp, msgs in records.items():
            for msg in msgs:
                # Перевіряємо кожне повідомлення на відповідність нашому ID
                if msg.value.get('meta', {}).get('id') == unique_msg_id:
                    found_msg = msg.value
                    break
            if found_msg: break
        if found_msg: break

    assert found_msg is not None, f"ID {unique_msg_id} не знайдено за {timeout_limit}с. Перевір, чи працює генератор."
    assert found_msg['page_title'] == f'Test Page {unique_msg_id}'

def test_spark_reads_kafka_stream_and_transforms():
    """Тест чистої логіки трансформації (без затримок Kafka)."""
    test_data = {
        'meta': {'id': '550e8400-e29b-41d4-a716-446655440000',
                 'dt': '2026-05-01T10:30:00Z'},
        'page_title': 'Integration Test Page',
        'performer': {'user_text': 'IntegrationUser'}
    }
    transformed = transform_wikipedia_event(json.dumps(test_data))
    assert transformed['id'] == '550e8400-e29b-41d4-a716-446655440000'
    assert transformed['page_title'] == 'Integration Test Page'