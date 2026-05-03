"""
Spark Streaming Job 1: Filtering Wikipedia events
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

# Allowed domains
allowed_domains = ["en.wikipedia.org", "www.wikidata.org", "commons.wikimedia.org"]

# Reading input
df = spark.readStream \
    .format("kafka") \
    .option("subscribe", "input") \
    .load()

# Filtration:
# 1. domain з allowed_domains
# 2. user_is_bot = false
filtered_df = parsed_df \
    .filter(col("meta.domain").isin(allowed_domains)) \
    .filter(col("performer.user_is_bot") == False) \
    .select(
        col("performer.user_text").alias("user_id"),
        col("meta.domain").alias("domain"),
        col("meta.dt").alias("created_at"),
        col("page_title")
    )

# Writing into processed
query = filtered_df.writeStream \
    .format("kafka") \
    .option("topic", "processed") \
    .start()