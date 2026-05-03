import os
import json
import requests
import time
from kafka import KafkaProducer
from sseclient import SSEClient

from prometheus_client import Counter, Gauge, push_to_gateway, CollectorRegistry

def parse_wikipedia_event(raw_data):
    return json.loads(raw_data)

if __name__ == "__main__":
    kafka_broker = os.environ.get("KAFKA_BROKER", "localhost:9092")
    
    pushgateway_url = os.environ.get("PUSHGATEWAY_URL", "pushgateway:9091")
    
    # Metrics init
    registry = CollectorRegistry()
    messages_sent = Counter('generator_messages_sent', 'Total messages sent', registry=registry)
    generator_status = Gauge('generator_status', 'Generator health status', registry=registry)
    
    generator_status.set(1) 
    
    producer = KafkaProducer(
        bootstrap_servers=[kafka_broker],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        retries=5
    )
    
    url = "https://stream.wikimedia.org/v2/stream/page-create"
    headers = {'User-Agent': 'MyDataPipelineProject/1.0'}
    
    last_push_time = time.time()
    
    try:
        response = requests.get(url, stream=True, headers=headers, timeout=15)
        messages = SSEClient(response)
        
        for msg in messages.events():
            if msg.data:
                try:
                    data = parse_wikipedia_event(msg.data)
                    producer.send('input', value=data)
                    
                    
                    messages_sent.inc()
                    
                    # Sending metrics into Pushgetaway every 10 sec
                    if time.time() - last_push_time > 10:
                        try:
                            push_to_gateway(pushgateway_url, job='wikipedia_generator', registry=registry)
                            last_push_time = time.time()
                        except Exception as e:
                            print(f"Metrics push failed: {e}")
                            
                except Exception as e:
                    print(f"Error: {e}")
                    
    except KeyboardInterrupt:
        generator_status.set(0) 
        push_to_gateway(pushgateway_url, job='wikipedia_generator', registry=registry)
    finally:
        producer.close()