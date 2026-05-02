def test_processor_uses_consistent_connector_version():
    """
    Перевіряємо що processor.py використовує версію коннектора 3.5.0
    (відповідає Spark 3.5.0 у Dockerfile).
    """
    import os
    path = os.path.join(os.path.dirname(__file__), '../../src/spark/processor.py')
    
    with open(path, 'r') as f:
        content = f.read()
        
    assert 'spark-cassandra-connector_2.12:3.5.0' in content, \
        'Версія Cassandra connector повинна бути 3.5.0'
        
    assert 'spark-sql-kafka-0-10_2.12:3.5.0' in content, \
        'Версія Kafka connector повинна бути 3.5.0'
        
    assert '3.4.1' not in content, \
        'Стара версія 3.4.1 залишилась в processor.py!'