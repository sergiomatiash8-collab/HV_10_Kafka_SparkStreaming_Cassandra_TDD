from kafka import KafkaConsumer
import json
import time

def test_kafka_has_data():
    
    consumer = KafkaConsumer(
        'input',
        bootstrap_servers=['127.0.0.1:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=5000  
    )
    
    print("\n[TEST] Searching for messages in 'input'...")
    
    found_msg = None
    start_time = time.time()
    
    # Спробуємо отримати повідомлення протягом 10 секунд
    while time.time() - start_time < 10:
        found_msg = next(consumer, None)
        if found_msg:
            break
        time.sleep(1)
    
    try:
        if found_msg:
            print(f"[TEST] Success! Found message with title: {found_msg.value.get('page_title')}")
    
        assert found_msg is not None, "Topic 'input' is empty or unreachable!"
        assert 'page_title' in found_msg.value, "Field 'page_title' missing in message!"
        
    finally:
        consumer.close()