"""
Smoke test: Запис в Cassandra через Spark.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from datetime import datetime
from cassandra.cluster import Cluster
import time


def wait_for_cassandra():
    """Чекає Cassandra 60 секунд."""
    for _ in range(12):
        try:
            cluster = Cluster(['cassandra'], port=9042)
            session = cluster.connect()
            cluster.shutdown()
            return True
        except Exception:
            time.sleep(5)
    return False


def test_cassandra_setup():
    """Перевіряє що Cassandra готова."""
    assert wait_for_cassandra(), "Cassandra не готова після 60 секунд!"


def test_write_to_cassandra():
    """
    Тест запису в Cassandra.
    """
    assert wait_for_cassandra(), "Cassandra недоступна!"
    
    spark = SparkSession.builder \
        .appName("CassandraWriteTest") \
        .config("spark.jars.packages", 
                "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .master("local[1]") \
        .getOrCreate()

    # Тестові дані з UUID як string
    test_data = [Row(
        id="550e8400-e29b-41d4-a716-446655440099",
        page_title="Smoke Test Page",
        user_text="SmokeTestUser",
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
        print("✅ Запис успішний!")
        
    except Exception as e:
        print(f"❌ Помилка: {e}")
        import traceback
        traceback.print_exc()
        success = False
    finally:
        spark.stop()
    
    assert success, "Запис в Cassandra не вдався!"