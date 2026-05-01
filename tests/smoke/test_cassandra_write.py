import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from datetime import datetime

def test_actual_cassandra_write():
    """
    Інтеграційний тест: перевіряємо, чи може Spark записати рядок у Cassandra.
    Цей тест буде запускатися ТІЛЬКИ всередині Docker.
    """
    spark = SparkSession.builder \
        .appName("CassandraIntegrationTest") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .getOrCreate()

    # Створюємо тестовий рядок даних
    test_data = [Row(
        id="550e8400-e29b-41d4-a716-446655440000", 
        page_title="Test Page", 
        user_text="Test User", 
        dt=datetime.now()
    )]
    
    df = spark.createDataFrame(test_data)

    # Пробуємо записати в Cassandra
    try:
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="edits", keyspace="wiki_namespace") \
            .mode("append") \
            .save()
        success = True
    except Exception as e:
        print(f"Помилка запису: {e}")
        success = False

    spark.stop()
    assert success is True