from kafka import KafkaConsumer
import json
import time

def test_kafka_has_data():
    """Перевірка, чи є в топіку хоча б одне повідомлення"""
    
    # Використовуємо порт 9092 для доступу ззовні контейнера
    consumer = KafkaConsumer(
        'input',
        bootstrap_servers=['127.0.0.1:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=10000  # Чекаємо 10 секунд
    )
    
    print("\n[TEST] Searching for messages in 'input'...")
    
    # Спробуємо отримати повідомлення
    msg = next(consumer, None)
    
    if msg:
        print(f"[TEST] Success! Found message with title: {msg.value.get('page_title')}")
    
    # Перевірки
    assert msg is not None, "Topic 'input' is empty or unreachable!"
    assert 'page_title' in msg.value, "Field 'page_title' missing in message!"
    
    consumer.close()