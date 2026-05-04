from kafka import KafkaAdminClient
from kafka.admin import NewTopic

def test_required_topics_exist():
    
    
    
    bootstrap_servers = "127.0.0.1:9092"
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    
    try:
        existing_topics = set(admin_client.list_topics())
        required = {"input", "processed"}
        
        
        to_create = []
        for topic in required:
            if topic not in existing_topics:
                to_create.append(NewTopic(name=topic, num_partitions=1, replication_factor=1))
        
        if to_create:
            admin_client.create_topics(new_topics=to_create, validate_only=False)
            
            existing_topics = set(admin_client.list_topics())

        assert required.issubset(existing_topics), f"Topics are abscent: {required - existing_topics}"
    finally:
        admin_client.close()