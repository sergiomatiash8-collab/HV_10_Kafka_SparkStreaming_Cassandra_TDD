import pytest
import os
from pyspark.sql import SparkSession

def test_spark_kafka_packages_load(spark):
    """
    Checks that Spark has successfully loaded the packages required to work with Kafka.
    Uses the global “spark” session from conftest.py to avoid JVM conflicts on Windows.
    """
    try:
        
        
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
            .option("subscribe", "input") \
            .load()
        
        
        assert df.isStreaming
        
    except Exception as e:
        
        pytest.fail(f"Spark was unable to initialise the Kafka stream due to a global session: {e}")