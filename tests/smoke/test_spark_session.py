import pytest
import os
from pyspark.sql import SparkSession

def test_spark_kafka_packages_load(spark):
    """
    Перевіряє, що Spark успішно завантажив пакети для роботи з Kafka.
    Використовує глобальну сесію 'spark' з conftest.py, щоб уникнути 
    конфліктів JVM на Windows.
    """
    try:
        # Ми не створюємо нову сесію тут, а використовуємо ту, 
        # що вже має завантажені пакети Kafka та Cassandra.
        
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "input") \
            .load()
        
        # Якщо ми дійшли до цієї точки без AnalysisException, 
        # значить пакети підтягнулися успішно.
        assert df.isStreaming
        
    except Exception as e:
        # Виводимо зрозумілу помилку, якщо щось пішло не так
        pytest.fail(f"Spark не зміг ініціалізувати Kafka-стрім через глобальну сесію: {e}")

# Функція spark.stop() не потрібна тут, оскільки вона 
# виконується у фікстурі conftest.py після завершення всіх тестів.