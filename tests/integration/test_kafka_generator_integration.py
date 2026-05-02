"""
Integration test: Перевірка що Generator може писати в Kafka.
"""

import pytest
from kafka import KafkaConsumer, KafkaProducer
import json
import time


@pytest.fixture
def kafka_producer():
    """Fixture для Kafka producer."""
    producer = KafkaProducer(
        bootstrap_servers=['kafka:29092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    yield producer
    producer.close()


@pytest.fixture
def kafka_consumer():
    """Fixture для Kafka consumer."""
    consumer = KafkaConsumer(
        'input',
        bootstrap_servers=['kafka:29092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='test-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=5000
    )
    yield consumer
    consumer.close()


def test_kafka_producer_can_send_message(kafka_producer, kafka_consumer):
    """
    Перевіряє що можна відправити повідомлення в Kafka і прочитати його.
    """
    # Тестове повідомлення
    test_message = {
        "meta": {
            "id": "test-123",
            "dt": "2024-05-01T10:00:00Z"
        },
        "page_title": "Test Page",
        "performer": {
            "user_text": "TestUser"
        }
    }
    
    # Відправка
    kafka_producer.send('input', value=test_message)
    kafka_producer.flush()
    
    # Читання
    messages = []
    for message in kafka_consumer:
        messages.append(message.value)
        break  # Читаємо тільки 1 повідомлення
    
    assert len(messages) > 0, "Не вдалося прочитати повідомлення з Kafka!"
    assert messages[0]['page_title'] == "Test Page"


def test_spark_reads_kafka_stream_and_transforms(kafka_producer):
    """
    Перевіряє що Spark може читати з Kafka і трансформувати дані.
    
    Цей тест НЕ запускає Spark job (це робить окремий контейнер),
    а лише перевіряє що дані в правильному форматі.
    """
    from src.spark.logic import transform_wikipedia_event
    
    # Тестові дані
    test_data = {
        "meta": {
            "id": "550e8400-e29b-41d4-a716-446655440000",
            "dt": "2024-05-01T10:30:00Z"
        },
        "page_title": "Integration Test Page",
        "performer": {
            "user_text": "IntegrationUser"
        }
    }
    
    # Відправка в Kafka
    kafka_producer.send('input', value=test_data)
    kafka_producer.flush()
    
    # Трансформація (як робить Spark)
    raw_json = json.dumps(test_data)
    transformed = transform_wikipedia_event(raw_json)
    
    # Перевірка результату
    assert transformed is not None
    assert transformed['id'] == "550e8400-e29b-41d4-a716-446655440000"
    assert transformed['page_title'] == "Integration Test Page"
    assert transformed['user_text'] == "IntegrationUser"
    assert transformed['dt'] == "2024-05-01T10:30:00Z"