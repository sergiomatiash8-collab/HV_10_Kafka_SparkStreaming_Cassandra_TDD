import json
import time
import pytest
import requests
import urllib3
from kafka import KafkaConsumer, KafkaProducer
from sseclient import SSEClient
from src.generator.generator import parse_wikipedia_event


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


KAFKA_BROKER = 'localhost:9092'
TOPIC = 'input'
WIKIPEDIA_SSE_URL = 'https://stream.wikimedia.org/v2/stream/page-create'
HEADERS = {'User-Agent': 'HV10-E2E-Test/1.0 (test@example.com)'}

def test_wikipedia_event_reaches_kafka():
    
    
   
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        retries=5
    )

    
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='latest',
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=30000 
    )

    
    print("\n[E2E] Warming up consumer...")
    consumer.poll(timeout_ms=3000)

    print("[E2E] Connecting to Wikipedia SSE (waiting for events)...")
    caught_event = None
    
    try:
        
        with requests.get(WIKIPEDIA_SSE_URL, stream=True, headers=HEADERS, timeout=30, verify=False) as response:
            client = SSEClient(response)
            for event in client.events():
                if event.data:
                    parsed_data = parse_wikipedia_event(event.data)
                    
                    if parsed_data:
                       
                        future = producer.send(TOPIC, value=parsed_data)
                        record_metadata = future.get(timeout=15)
                        producer.flush()
                        
                        caught_event = parsed_data
                        event_id = parsed_data.get('meta', {}).get('id')
                        print(f"[E2E] Successfully sent event to Kafka. ID: {event_id}")
                        break
    except Exception as e:
        pytest.fail(f"Wikipedia Stream Error: {e}")
    finally:
        producer.close()

    assert caught_event is not None, "Failed to capture any event from Wikipedia"
    target_id = caught_event['meta']['id']

    
    print(f"[E2E] Searching for event ID {target_id} in Kafka...")
    received_msg = None
    
    
    start_wait = time.time()
    while time.time() - start_wait < 30:
        records = consumer.poll(timeout_ms=1000)
        for tp, msgs in records.items():
            for msg in msgs:
                if msg.value.get('meta', {}).get('id') == target_id:
                    received_msg = msg.value
                    break
        if received_msg:
            break

    consumer.close()

    
    assert received_msg is not None, f"Event {target_id} was sent but not found in Kafka topic '{TOPIC}'"
    assert received_msg['page_title'] == caught_event['page_title']
    
    print(f"[E2E]  SUCCESS: Event '{received_msg['page_title']}' processed correctly.")