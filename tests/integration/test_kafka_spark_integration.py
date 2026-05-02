"""Integration test: Kafka → Spark (batch read)."""
import pytest

# Константи
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'input' # Виправлено: додано пропущений TOPIC

def test_spark_reads_kafka_stream(spark):
    """
    Spark читає з Kafka в batch-режимі і перевіряє схему.
    Використовує глобальну сесію 'spark' з conftest.py.
    """
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
            assert col in df.columns, f'Відсутня колонка: {col}'
            
        count = df.count()
        print(f'Знайдено {count} повідомлень в топіку {TOPIC}')
        assert count >= 0
        
    except Exception as e:
        pytest.fail(f'Не вдалося прочитати з Kafka: {e}')