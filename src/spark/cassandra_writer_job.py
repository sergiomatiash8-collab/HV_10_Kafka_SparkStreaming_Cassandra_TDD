"""
Spark Streaming Job 2: Запис в Cassandra
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp

# Read from processed
df = spark.readStream \
    .format("kafka") \
    .option("subscribe", "processed") \
    .load()

# Convert created_at into timestamp
final_df = parsed_df \
    .withColumn("created_at", to_timestamp(col("created_at")))

# Writing into Cassandra
query = final_df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="edits", keyspace="wiki_namespace") \
    .start()