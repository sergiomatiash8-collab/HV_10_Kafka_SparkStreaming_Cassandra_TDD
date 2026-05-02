"""
Integration test: Spark → Cassandra запис/читання.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from datetime import datetime
from cassandra.cluster import Cluster
import time


@pytest.fixture(scope="module")
def spark():
    """Spark session з Cassandra connector."""
    spark = SparkSession.builder \
        .appName("SparkCassandraIntegrationTest") \
        .config("spark.jars.packages", 
                "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .master("local[1]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture
def cassandra_session():
    """Cassandra session для перевірки даних."""
    max_retries = 10
    for attempt in range(max_retries):
        try:
            cluster = Cluster(['cassandra'], port=9042)
            session = cluster.connect('wiki_namespace')
            yield session
            cluster.shutdown()
            return
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(3)
            else:
                pytest.fail(f"Cassandra недоступна: {e}")


def test_spark_writes_row_to_cassandra(spark, cassandra_session):
    """
    Тестує запис даних з Spark в Cassandra.
    """
    # Очищаємо таблицю перед тестом
    cassandra_session.execute("TRUNCATE wiki_namespace.edits")
    
    # Створюємо тестовий DataFrame
    test_id = "550e8400-e29b-41d4-a716-446655440001"
    test_data = [Row(
        id=test_id,
        page_title="Spark Integration Test Page",
        user_text="SparkTestUser",
        dt=datetime(2024, 5, 1, 12, 0, 0)
    )]
    
    df = spark.createDataFrame(test_data)
    
    # Запис в Cassandra
    try:
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="edits", keyspace="wiki_namespace") \
            .mode("append") \
            .save()
        
        print("✅ Запис в Cassandra успішний!")
        
    except Exception as e:
        pytest.fail(f"Помилка запису в Cassandra: {e}")
    
    # Перевірка через CQL
    time.sleep(2)  # Даємо час на запис
    
    result = cassandra_session.execute(
        f"SELECT * FROM wiki_namespace.edits WHERE id = {test_id}"
    )
    
    rows = list(result)
    assert len(rows) == 1, "Запис не знайдено в Cassandra!"
    assert rows[0].page_title == "Spark Integration Test Page"


def test_spark_reads_back_written_row(spark):
    """
    Тестує читання даних з Cassandra через Spark.
    """
    # Читаємо дані які записали в попередньому тесті
    df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="edits", keyspace="wiki_namespace") \
        .load()
    
    # Фільтруємо тестовий запис
    test_df = df.filter(df.page_title == "Spark Integration Test Page")
    
    count = test_df.count()
    assert count >= 1, "Не вдалося прочитати записані дані!"
    
    # Перевіряємо структуру
    row = test_df.first()
    assert row.user_text == "SparkTestUser"
    assert row.page_title == "Spark Integration Test Page"


def test_duplicate_id_is_upserted(spark, cassandra_session):
    """
    Тестує що дублікат ID оновлює запис (upsert).
    """
    test_id = "550e8400-e29b-41d4-a716-446655440002"
    
    # Перший запис
    data1 = [Row(
        id=test_id,
        page_title="Original Title",
        user_text="OriginalUser",
        dt=datetime(2024, 5, 1, 13, 0, 0)
    )]
    
    spark.createDataFrame(data1).write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="edits", keyspace="wiki_namespace") \
        .mode("append") \
        .save()
    
    time.sleep(1)
    
    # Другий запис з тим самим ID
    data2 = [Row(
        id=test_id,
        page_title="Updated Title",
        user_text="UpdatedUser",
        dt=datetime(2024, 5, 1, 14, 0, 0)
    )]
    
    spark.createDataFrame(data2).write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="edits", keyspace="wiki_namespace") \
        .mode("append") \
        .save()
    
    time.sleep(2)
    
    # Перевірка - має бути тільки 1 запис (upsert)
    result = cassandra_session.execute(
        f"SELECT * FROM wiki_namespace.edits WHERE id = {test_id}"
    )
    
    rows = list(result)
    assert len(rows) == 1, "Має бути тільки 1 запис (upsert)!"
    assert rows[0].page_title == "Updated Title", "Запис не оновився!"