import pytest
import os
import shutil
import uuid
import time
from datetime import datetime
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col
from cassandra.cluster import Cluster

# Налаштування шляхів для Windows
TMP_DIR = "C:/spark_tmp/cassandra_test"

def setup_cassandra_schema():
    """Створює Keyspace та Table перед тестом."""
    host = '127.0.0.1'
    try:
        cluster = Cluster([host], port=9042)
        session = cluster.connect()
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS wiki_namespace 
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
        """)
        session.execute("""
            CREATE TABLE IF NOT EXISTS wiki_namespace.edits (
                id uuid PRIMARY KEY,
                page_title text,
                user_text text,
                dt timestamp
            );
        """)
        cluster.shutdown()
        return True
    except Exception as e:
        print(f"❌ Помилка схеми: {e}")
        return False

@pytest.fixture(scope="module")
def spark():
    if os.path.exists(TMP_DIR):
        shutil.rmtree(TMP_DIR, ignore_errors=True)
    os.makedirs(TMP_DIR, exist_ok=True)

    s = (SparkSession.builder
        .appName("CassandraSmokeTest")
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0")
        .config("spark.cassandra.connection.host", "127.0.0.1")
        .config("spark.cassandra.connection.port", "9042")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.local.dir", TMP_DIR)
        .config("spark.hadoop.fs.permissions.umask-mode", "000")
        .master("local[1]")
        .getOrCreate())
    
    yield s
    s.stop()
    time.sleep(2) # Даємо Windows час закрити файли
    shutil.rmtree(TMP_DIR, ignore_errors=True)

def test_cassandra_setup():
    assert setup_cassandra_schema()

def test_write_to_cassandra(spark):
    # 1. Працюємо ТІЛЬКИ з рядком
    test_id_str = str(uuid.uuid4()) 
    
    test_data = [Row(
        id=test_id_str, 
        page_title="Windows Test Page", 
        user_text="tester", 
        dt=datetime.now()
    )]
    
    df = spark.createDataFrame(test_data)

    try:
        # 2. Запис (конектор сам конвертує string -> uuid в Cassandra)
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="edits", keyspace="wiki_namespace") \
            .mode("append") \
            .save()
        
        # 3. Читання та Фільтрація (використовуємо ТІЛЬКИ рядок)
        # Це критично: фільтруємо за рядком, щоб Py4J не впав
        read_df = spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="edits", keyspace="wiki_namespace") \
            .load() \
            .filter(col("id") == test_id_str) # ТУТ МАЄ БУТИ РЯДОК
        
        count = read_df.count()
        assert count >= 1
        print(f"✅ Успіх! Знайдено записів: {count} для ID: {test_id_str}")
        
    except Exception as e:
        # Виводимо повний трейсбек, якщо впаде
        import traceback
        print(traceback.format_exc())
        pytest.fail(f"Помилка: {e}")