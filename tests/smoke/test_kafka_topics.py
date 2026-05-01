from kafka import KafkaAdminClient

def test_required_topics_exist():
    """Перевірка наявності топіків input та processed"""
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092")
    topics = admin_client.list_topics()
    required = {"input", "processed"}
    
    # Перевіряємо, чи є наші топіки серед усіх топіків у Kafka
    assert required.issubset(set(topics)), f"Відсутні топіки: {required - set(topics)}"
    admin_client.close()