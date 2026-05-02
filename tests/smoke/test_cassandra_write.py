import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col
from datetime import datetime
import time
import uuid

def wait_for_cassandra(max_retries=10):
    """Чекає поки Cassandra стане доступною за локальною адресою."""
    from cassandra.cluster import Cluster
    
    # Для Windows/Local використовуємо 127.0.0.1
    host = '127.0.0.1'
    for attempt in range(max_retries):
        try:
            cluster = Cluster([host], port=9042)
            session = cluster.connect()
            cluster.shutdown()
            return True
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Cassandra на {host} не готова, чекаємо... ({attempt + 1}/{max_retries})")
                time.sleep(5)
            else:
                return False
    return False

def test_cassandra_reachable():
    assert wait_for_cassandra(), "Cassandra недоступна на localhost:9042!"

def test_write_to_cassandra():
    assert wait_for_cassandra()
    
    # Створюємо Spark session з підключенням до 127.0.0.1
    spark = SparkSession.builder \
        .appName("CassandraWriteTest") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
        .config("spark.cassandra.connection.host", "127.0.0.1") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
        .master("local[1]") \
        .getOrCreate()

    # Тестові дані: важливо, щоб id був рядком, який Spark зможе записати в UUID
    test_data = [Row(
        id=str(uuid.uuid4()), 
        page_title="Test Wikipedia Page", 
        user_text="TestUser", 
        dt=datetime.now()
    )]
    
    df = spark.createDataFrame(test_data)

    try:
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="edits", keyspace="wiki_namespace") \
            .mode("append") \
            .save()
        success = True
    except Exception as e:
        print(f"❌ Помилка запису: {e}")
        success = False
    finally:
        spark.stop()

    assert success, "Запис в Cassandra не вдався!"

def test_read_from_cassandra():
    assert wait_for_cassandra()
    
    spark = SparkSession.builder \
        .appName("CassandraReadTest") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
        .config("spark.cassandra.connection.host", "127.0.0.1") \
        .config("spark.cassandra.connection.port", "9042") \
        .master("local[1]") \
        .getOrCreate()

    try:
        df = spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="edits", keyspace="wiki_namespace") \
            .load()
        
        count = df.count()
        print(f"📊 Знайдено {count} записів")
        assert count >= 1
    finally:
        spark.stop()