import json
import time
import pytest
import requests
import threading
from kafka import KafkaConsumer, KafkaProducer
from sseclient import SSEClient
from src.generator.generator import parse_wikipedia_event

KAFKA_BROKER = 'localhost:9092'
TOPIC = 'input'
WIKIPEDIA_SSE_URL = 'https://stream.wikimedia.org/v2/stream/page-create'
# Обов'язково додаємо User-Agent за правилами Wikimedia API
HEADERS = {'User-Agent': 'HV10-E2E-Test/1.0 (test@example.com)'}

def test_wikipedia_event_reaches_kafka():
    """
    Перевіряємо, що ми можемо отримати реальну подію з Wikipedia 
    та успішно відправити її в наш локальний Kafka топік.
    """
    received_events = []
    stop_flag = threading.Event()

    # 1. Функція для фонового читання з Kafka
    def consume():
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=5000  # Зупиняємося, якщо 5 сек немає даних
        )
        
        # Чекаємо призначення партицій
        for _ in range(10):
            consumer.poll(timeout_ms=500)
            if consumer.assignment():
                break
        
        for msg in consumer:
            received_events.append(msg.value)
            if stop_flag.is_set():
                break
        consumer.close()

    # Стартуємо консьюмер у окремому потоці
    thread = threading.Thread(target=consume, daemon=True)
    thread.start()
    time.sleep(2) # Даємо час на коннект до Kafka

    # 2. Отримуємо реальну подію з Wikipedia та відправляємо в Kafka
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    print("\n[E2E] Підключення до Wikipedia SSE...")
    try:
        response = requests.get(WIKIPEDIA_SSE_URL, stream=True, headers=HEADERS, timeout=15)
        client = SSEClient(response)
        
        for event in client.events():
            if event.data:
                # Використовуємо твою функцію парсингу
                parsed_data = parse_wikipedia_event(event.data)
                if parsed_data:
                    producer.send(TOPIC, value=parsed_data)
                    producer.flush()
                    print(f"[E2E] Спіймали та відправили подію: {parsed_data.get('page_title')}")
                    break # Нам достатньо однієї реальної події для тесту
                    
        response.close()
    except Exception as e:
        pytest.fail(f"Не вдалося отримати дані з Wikipedia: {e}")
    finally:
        producer.close()

    # 3. Чекаємо, поки консьюмер підхопить повідомлення
    time.sleep(3)
    stop_flag.set()
    thread.join(timeout=5)

    # Перевірки
    assert len(received_events) >= 1, "Жодна подія з Wikipedia не дійшла до Kafka"
    
    msg = received_events[0]
    # Перевіряємо структуру, яку має видавати твій parse_wikipedia_event
    assert 'meta' in msg, "Відсутній блок 'meta'"
    assert 'id' in msg['meta'], "У метаданих немає id"
    assert 'page_title' in msg, "Відсутнє поле page_title"
    
    print(f"[E2E] Тест успішний! Подія '{msg['page_title']}' пройшла повний шлях до Kafka.")