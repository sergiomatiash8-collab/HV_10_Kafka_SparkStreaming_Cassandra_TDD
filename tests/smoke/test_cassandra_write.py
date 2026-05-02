import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime

def test_manual_cassandra_write():
    # Налаштовуємо Spark для роботи всередині Docker-мережі
    spark = SparkSession.builder \
        .appName("SmokeTest-CassandraWrite") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()

    try:
        # Використовуємо схему, яка відповідає вашій таблиці в Cassandra
        schema = StructType([
            StructField("id", StringType(), False),
            StructField("page_title", StringType(), True),
            StructField("user_text", StringType(), True),
            StructField("dt", TimestampType(), True)
        ])
        
        # Тестовий запис
        data = [("smoke-id-100", "TDD Victory", "docker-user", datetime.now())]
        df = spark.createDataFrame(data, schema)

        # Запис у Cassandra через ім'я сервісу 'cassandra'
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="edits", keyspace="wiki_namespace") \
            .mode("append") \
            .save()

        # Перевірка: чи можемо ми прочитати цей запис назад?
        read_df = spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="edits", keyspace="wiki_namespace") \
            .load()

        assert read_df.filter(read_df.id == "smoke-id-100").count() > 0
        print("\n✅ Дані успішно записані та зчитані з Cassandra!")

    except Exception as e:
        pytest.fail(f"Smoke-тест провалився: {e}")
        
    finally:
        spark.stop()