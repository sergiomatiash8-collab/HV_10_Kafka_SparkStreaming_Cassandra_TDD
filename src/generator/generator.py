import os
import json
import requests
import time
from kafka import KafkaProducer
from sseclient import SSEClient

def parse_wikipedia_event(raw_data):
    return json.loads(raw_data)

if __name__ == "__main__":
    # Динамічно отримуємо адресу Kafka з Docker або використовуємо localhost
    kafka_broker = os.environ.get("KAFKA_BROKER", "localhost:9092")
    
    print(f"Ініціалізація Kafka Producer (Broker: {kafka_broker})...")
    
    producer = KafkaProducer(
        bootstrap_servers=[kafka_broker],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        retries=5
    )
    
    url = "https://stream.wikimedia.org/v2/stream/page-create"
    headers = {'User-Agent': 'MyDataPipelineProject/1.0 (contact: your_email@example.com)'}
    
    try:
        print("Підключення до стріму Wikipedia...")
        response = requests.get(url, stream=True, headers=headers, timeout=15)
        messages = SSEClient(response)
        
        print("Початок стрімінгу в топік 'input'...")
        
        for msg in messages.events():
            if msg.data:
                try:
                    data = parse_wikipedia_event(msg.data)
                    # Відправка даних
                    producer.send('input', value=data)
                    
                    title = data.get('page_title', 'Unknown Title')
                    print(f"✅ Sent to Kafka: {title}")
                    
                except json.JSONDecodeError:
                    continue
                except Exception as e:
                    print(f"❌ Помилка при відправці: {e}")
                    
    except KeyboardInterrupt:
        print("\nЗупинка генератора...")
    finally:
        producer.close()
        print("Kafka Producer закрито.")