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

HEADERS = {'User-Agent': 'HV10-E2E-Test/1.0 (test@example.com)'}

def test_wikipedia_event_reaches_kafka():
    """
    We verify that we can get a real event from Wikipedia 
and successfully send it to our local Kafka topic.
    """
    received_events = []
    stop_flag = threading.Event()

    
    def consume():
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=5000  
        )
        
        
        for _ in range(10):
            consumer.poll(timeout_ms=500)
            if consumer.assignment():
                break
        
        for msg in consumer:
            received_events.append(msg.value)
            if stop_flag.is_set():
                break
        consumer.close()

    
    thread = threading.Thread(target=consume, daemon=True)
    thread.start()
    time.sleep(2) 

    
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    print("\n[E2E] Connecting to Wikipedia SSE...")
    try:
        response = requests.get(WIKIPEDIA_SSE_URL, stream=True, headers=HEADERS, timeout=15)
        client = SSEClient(response)
        
        for event in client.events():
            if event.data:
                
                parsed_data = parse_wikipedia_event(event.data)
                if parsed_data:
                    producer.send(TOPIC, value=parsed_data)
                    producer.flush()
                    print(f"[E2E] Caught and sent an event: {parsed_data.get('page_title')}")
                    break 
                    
        response.close()
    except Exception as e:
        pytest.fail(f"Failed to retrieve data from Wikipedia: {e}")
    finally:
        producer.close()

    
    time.sleep(3)
    stop_flag.set()
    thread.join(timeout=5)

    
    assert len(received_events) >= 1, "No event from Wikipedia reached Kafka"
    
    msg = received_events[0]
    
    assert 'meta' in msg, "Block 'meta' is abscent"
    assert 'id' in msg['meta'], "Metadata doesn't have id"
    assert 'page_title' in msg, "No field page_title"
    
    print(f"[E2E] Test is successful! Event '{msg['page_title']}' ended in Kafka.")