"""
Unit test: Перевірка можливості створення Spark session з Cassandra.
"""

import pytest
from pyspark.sql import SparkSession


def test_spark_cassandra_connection():
    """
    Перевіряє чи Spark може створити session з Cassandra connector.
    """
    try:
        spark = SparkSession.builder \
            .appName("CassandraConnectionTest") \
            .config("spark.jars.packages", 
                    "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
            .config("spark.cassandra.connection.host", "localhost") \
            .config("spark.cassandra.connection.port", "9042") \
            .master("local[1]") \
            .getOrCreate()
        
        assert spark is not None, "Spark session не створився!"
        
        # Перевірка чи завантажився connector
        jvm_packages = spark._jvm.Thread.currentThread().getContextClassLoader()
        assert jvm_packages is not None
        
        spark.stop()
        
    except Exception as e:
        pytest.fail(f"Помилка створення Spark session: {e}")


def test_cassandra_connector_version():
    """
    Перевіряє що версія connector правильна.
    """
    spark = SparkSession.builder \
        .appName("VersionTest") \
        .config("spark.jars.packages", 
                "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
        .master("local[1]") \
        .getOrCreate()
    
    # Перевірка версії Spark
    assert spark.version.startswith("3.5"), f"Очікувалась Spark 3.5.x, отримано {spark.version}"
    
    spark.stop()