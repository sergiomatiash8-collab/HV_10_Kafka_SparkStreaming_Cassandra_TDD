"""
Smoke test: Перевірка запису в Cassandra.
Цей тест запускається ТІЛЬКИ всередині Docker після старту всіх сервісів.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from datetime import datetime
import time


def wait_for_cassandra(max_retries=10):
    """
    Чекає поки Cassandra стане доступною.
    """
    from cassandra.cluster import Cluster
    
    for attempt in range(max_retries):
        try:
            cluster = Cluster(['cassandra'], port=9042)
            session = cluster.connect()
            cluster.shutdown()
            return True
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Cassandra не готова, чекаємо... ({attempt + 1}/{max_retries})")
                time.sleep(5)
            else:
                return False
    return False


def test_cassandra_reachable():
    """
    Перевіряє чи Cassandra доступна.
    """
    assert wait_for_cassandra(), "Cassandra недоступна після 50 секунд очікування!"


def test_write_to_cassandra():
    """
    Інтеграційний тест: записує тестовий рядок в Cassandra.
    """
    # Чекаємо Cassandra
    assert wait_for_cassandra(), "Cassandra недоступна!"
    
    # Створюємо Spark session
    spark = SparkSession.builder \
        .appName("CassandraWriteTest") \
        .config("spark.jars.packages", 
                "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .master("local[1]") \
        .getOrCreate()

    # Тестові дані
    test_data = [Row(
        id="550e8400-e29b-41d4-a716-446655440000", 
        page_title="Test Wikipedia Page", 
        user_text="TestUser", 
        dt=datetime.now()
    )]
    
    df = spark.createDataFrame(test_data)

    # Спроба запису
    try:
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="edits", keyspace="wiki_namespace") \
            .mode("append") \
            .save()
        
        success = True
        print("✅ Запис в Cassandra успішний!")
        
    except Exception as e:
        print(f"❌ Помилка запису: {e}")
        import traceback
        traceback.print_exc()
        success = False

    spark.stop()
    assert success, "Запис в Cassandra не вдався!"


def test_read_from_cassandra():
    """
    Тест: читає дані з Cassandra.
    """
    assert wait_for_cassandra(), "Cassandra недоступна!"
    
    spark = SparkSession.builder \
        .appName("CassandraReadTest") \
        .config("spark.jars.packages", 
                "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .master("local[1]") \
        .getOrCreate()

    try:
        # Читаємо дані з таблиці
        df = spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="edits", keyspace="wiki_namespace") \
            .load()
        
        # Перевіряємо що можемо прочитати
        count = df.count()
        print(f"📊 Знайдено {count} записів в Cassandra")
        
        # Має бути хоча б 1 запис (з попереднього тесту)
        assert count >= 1, "В Cassandra мало бути хоча б 1 запис!"
        
        spark.stop()
        
    except Exception as e:
        spark.stop()
        pytest.fail(f"Не вдалося прочитати з Cassandra: {e}")