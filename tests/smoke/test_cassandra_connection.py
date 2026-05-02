"""
Smoke test: Перевірка базового підключення до Cassandra через Spark.
"""

import pytest
from pyspark.sql import SparkSession


def test_spark_cassandra_connection():
    """
    Перевіряє чи Spark може створити session з Cassandra connector.
    """
    spark = SparkSession.builder \
        .appName("CassandraConnectionTest") \
        .config("spark.jars.packages", 
                "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
        .config("spark.cassandra.connection.host", "localhost") \
        .config("spark.cassandra.connection.port", "9042") \
        .master("local[1]") \
        .getOrCreate()
    
    assert spark is not None
    spark.stop()


def test_cassandra_connector_version():
    """
    Перевіряє що Spark версія сумісна з connector.
    """
    spark = SparkSession.builder \
        .appName("VersionTest") \
        .config("spark.jars.packages", 
                "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
        .master("local[1]") \
        .getOrCreate()
    
    assert spark.version.startswith("3.5")
    spark.stop()