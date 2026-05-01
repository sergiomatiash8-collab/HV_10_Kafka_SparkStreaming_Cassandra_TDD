import pytest
from pyspark.sql import SparkSession

def test_spark_cassandra_connection():
    """
    Перевірка, чи Spark може ініціалізуватися з підтримкою Cassandra.
    Це 'тест на випередження', який провалиться, якщо немає потрібних пакетів.
    """
    try:
        spark = SparkSession.builder \
            .appName("CassandraTest") \
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
            .config("spark.cassandra.connection.host", "localhost") \
            .getOrCreate()
        
        # Якщо ми дісталися сюди, сесія створилася
        assert spark is not None
        spark.stop()
    except Exception as e:
        pytest.fail(f"Spark не зміг ініціалізуватися для Cassandra: {e}")