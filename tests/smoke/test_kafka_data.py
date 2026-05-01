from kafka import KafkaConsumer
import json

def test_kafka_has_data():
    """Перевірка, чи є в топіку хоча б одне повідомлення"""
    consumer = KafkaConsumer(
        'input',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=5000  # Чекаємо 5 секунд
    )
    
    msg = next(consumer, None)
    assert msg is not None, "Топік 'input' порожній!"
    assert 'page_title' in msg.value, "У повідомленні немає ключа page_title"
    consumer.close()