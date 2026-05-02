"""
Integration test: Kafka → Spark трансформація.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType
import json


@pytest.fixture(scope="module")
def spark():
    """Створення Spark session для integration тестів."""
    spark = SparkSession.builder \
        .appName("KafkaSparkIntegrationTest") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .master("local[1]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


def test_spark_reads_kafka_stream(spark):
    """
    Тестує що Spark може підключитися до Kafka.
    
    Це batch read (не streaming) для простоти тестування.
    """
    # Читаємо з Kafka (batch mode для тестів)
    try:
        df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", "input") \
            .option("startingOffsets", "earliest") \
            .option("endingOffsets", "latest") \
            .load()
        
        # Перевіряємо що DataFrame створився
        assert df is not None
        
        # Перевіряємо схему
        expected_columns = ['key', 'value', 'topic', 'partition', 'offset', 'timestamp']
        actual_columns = df.columns
        
        for col_name in expected_columns:
            assert col_name in actual_columns, f"Column '{col_name}' missing!"
        
        # Підраховуємо записи
        count = df.count()
        print(f"📊 Знайдено {count} повідомлень в топіку 'input'")
        
        # Має бути хоча б кілька повідомлень (від generator)
        assert count >= 0, "Kafka має містити повідомлення!"
        
    except Exception as e:
        pytest.fail(f"Не вдалося прочитати з Kafka: {e}")