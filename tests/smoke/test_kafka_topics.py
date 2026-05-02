from kafka import KafkaAdminClient
from kafka.admin import NewTopic

def test_required_topics_exist():
    """Перевірка та створення топіків input та processed"""
    
    # Для Windows використовуємо localhost:9092
    bootstrap_servers = "127.0.0.1:9092"
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    
    try:
        existing_topics = set(admin_client.list_topics())
        required = {"input", "processed"}
        
        # Створюємо топіки, яких не вистачає
        to_create = []
        for topic in required:
            if topic not in existing_topics:
                to_create.append(NewTopic(name=topic, num_partitions=1, replication_factor=1))
        
        if to_create:
            admin_client.create_topics(new_topics=to_create, validate_only=False)
            # Оновлюємо список після створення
            existing_topics = set(admin_client.list_topics())

        assert required.issubset(existing_topics), f"Відсутні топіки: {required - existing_topics}"
    finally:
        admin_client.close()