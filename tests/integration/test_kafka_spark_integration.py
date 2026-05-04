"""Integration test: Kafka → Spark (batch read)."""
import pytest


KAFKA_BROKER = 'localhost:9092'
TOPIC = 'input' 

def test_spark_reads_kafka_stream(spark):
    
    try:
        df = (
            spark.read
            .format('kafka')
            .option('kafka.bootstrap.servers', KAFKA_BROKER)
            .option('subscribe', TOPIC)
            .option('startingOffsets', 'earliest')
            .option('endingOffsets', 'latest')
            .load()
        )
        
        assert df is not None
        expected = ['key', 'value', 'topic', 'partition', 'offset', 'timestamp']
        for col in expected:
            assert col in df.columns, f'Col abscent: {col}'
            
        count = df.count()
        print(f'Found {count} masseges in topic {TOPIC}')
        assert count >= 0
        
    except Exception as e:
        pytest.fail(f'Unable to read from Kafka: {e}')