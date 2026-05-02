"""
Spark Streaming processor для Wikipedia events.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType

from src.spark.logic import transform_wikipedia_event
from src.spark.uuid_utils import string_to_uuid


def create_spark_session(kafka_broker):
    """Створює Spark session."""
    return (SparkSession.builder
        .appName("WikipediaStreamProcessor")
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0")
        .config("spark.cassandra.connection.host", "cassandra")
        .config("spark.cassandra.connection.port", "9042")
        .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.cassandra.output.consistency.level", "ONE")
        .master("local[*]")
        .getOrCreate())


if __name__ == "__main__":
    kafka_broker = os.environ.get("KAFKA_BROKER", "kafka:29092")
    spark = create_spark_session(kafka_broker)
    spark.sparkContext.setLogLevel("WARN")

    logic_schema = StructType([
        StructField("id", StringType(), True),
        StructField("page_title", StringType(), True),
        StructField("user_text", StringType(), True),
        StructField("dt", StringType(), True)
    ])

    transform_udf = udf(transform_wikipedia_event, logic_schema)
    uuid_udf = udf(string_to_uuid, StringType())

    try:
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_broker) \
            .option("subscribe", "input") \
            .option("startingOffsets", "earliest") \
            .load()

        parsed_df = df.select(
            transform_udf(col("value").cast("string")).alias("data")
        ).select("data.*")
        
        final_df = parsed_df \
            .withColumn("id", uuid_udf(col("id"))) \
            .withColumn("dt", to_timestamp(col("dt"))) \
            .filter(col("id").isNotNull())

        query = final_df.writeStream \
            .outputMode("append") \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="edits", keyspace="wiki_namespace") \
            .option("checkpointLocation", "/tmp/spark_checkpoints") \
            .option("spark.cassandra.output.ignoreNulls", "true") \
            .start()

        print("✅ Spark Streaming розпочато!")
        print(f"📥 Kafka: {kafka_broker}/input")
        print("💾 Cassandra: wiki_namespace.edits")
        
        query.awaitTermination()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        spark.stop()