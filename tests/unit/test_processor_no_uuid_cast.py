def test_processor_does_not_cast_uuid():
    """
    Перевіряємо що processor.py не містить .cast("uuid")
    бо Spark не має нативного UUID типу і це ламає запис у Cassandra.
    """
    import os
    path = os.path.join(os.path.dirname(__file__), '../../src/spark/processor.py')
    
    with open(path, 'r') as f:
        content = f.read()
        
    assert '.cast("uuid")' not in content, \
        'processor.py містить .cast("uuid") — це ламає Cassandra writer!'