import os
import pytest
from pyspark.sql import SparkSession

def test_spark_kafka_packages_load():
    # Вказуємо шлях до папки hadoop, яку ти створив у проєкті
    # Склеюємо поточну директорію з папкою hadoop
    project_root = os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    hadoop_path = os.path.join(project_root, "hadoop")
    
    # Встановлюємо змінні оточення для Spark
    os.environ["HADOOP_HOME"] = hadoop_path
    os.environ["hadoop.home.dir"] = hadoop_path
    
    # Додаємо bin до PATH
    os.environ["PATH"] += os.pathsep + os.path.join(hadoop_path, "bin")

    spark = SparkSession.builder \
        .appName("TestKafkaConnection") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.driver.host", "localhost") \
        .master("local[*]") \
        .getOrCreate()
    
    try:
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "input") \
            .load()
        
        assert df.isStreaming
    finally:
        spark.stop()