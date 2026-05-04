import pytest
from pyspark.sql import SparkSession

def test_spark_cassandra_connection_and_version():
    spark = SparkSession.builder \
        .appName("CassandraConnectionTest") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
        .config("spark.cassandra.connection.host", "127.0.0.1") \
        .config("spark.cassandra.connection.port", "9042") \
        .master("local[1]") \
        .getOrCreate()
    
    try:
        assert spark is not None
        assert spark.version.startswith("3.5")
    finally:
        spark.stop()