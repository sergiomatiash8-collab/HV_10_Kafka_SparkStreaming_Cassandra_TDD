import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, struct, to_json
from pyspark.sql.types import StructType, StructField, StringType, BooleanType


def create_spark_session():
    
    return (SparkSession.builder
        .appName("WikipediaFilterJob")
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .config("spark.sql.shuffle.partitions", "2")
        .master("local[*]")
        .getOrCreate())


def get_wikipedia_schema():
    
    return StructType([
        StructField("meta", StructType([
            StructField("id", StringType(), True),
            StructField("dt", StringType(), True),
            StructField("domain", StringType(), True)
        ]), True),
        StructField("performer", StructType([
            StructField("user_text", StringType(), True),
            StructField("user_is_bot", BooleanType(), True)
        ]), True),
        StructField("page_title", StringType(), True)
    ])


if __name__ == "__main__":
    kafka_broker = os.environ.get("KAFKA_BROKER", "kafka:29092")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    
    allowed_domains = ["en.wikipedia.org", "www.wikidata.org", "commons.wikimedia.org"]

    try:
        print(" Starting Spark Filter Job...")
        print(f" Reading from: {kafka_broker}/input")
        print(f" Writing to: {kafka_broker}/processed")
        print(f"Filters: domains={allowed_domains}, user_is_bot=false")

        
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_broker) \
            .option("subscribe", "input") \
            .option("startingOffsets", "latest") \
            .load()

        
        schema = get_wikipedia_schema()
        parsed_df = df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")

        
        filtered_df = parsed_df \
            .filter(col("meta.domain").isin(allowed_domains)) \
            .filter(col("performer.user_is_bot") == False) \
            .select(
                col("performer.user_text").alias("user_id"),
                col("meta.domain").alias("domain"),
                col("meta.dt").alias("created_at"),
                col("page_title")
            )

        
        kafka_output = filtered_df.select(
            to_json(struct("user_id", "domain", "created_at", "page_title")).alias("value")
        )

        
        query = kafka_output.writeStream \
            .outputMode("append") \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_broker) \
            .option("topic", "processed") \
            .option("checkpointLocation", "/tmp/spark_checkpoints_filter") \
            .start()

        print(" Spark Filter Job started successfully!")
        
        query.awaitTermination()
        
    except Exception as e:
        print(f" Error: {e}")
        import traceback
        traceback.print_exc()
        spark.stop()