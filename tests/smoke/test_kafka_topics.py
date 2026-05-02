from kafka import KafkaAdminClient

def test_required_topics_exist():
    """Перевірка наявності лише топіка input (processed видалено згідно з архітектурою)"""
    
    # Використовуємо внутрішню мережу Docker та порт 29092
    admin_client = KafkaAdminClient(bootstrap_servers="kafka:29092")
    
    try:
        topics = admin_client.list_topics()
        # Залишаємо тільки input, бо запис іде в Cassandra
        required = {"input"}
        
        existing_topics = set(topics)
        assert required.issubset(existing_topics), f"Відсутні топіки: {required - existing_topics}"
    finally:
        admin_client.close()