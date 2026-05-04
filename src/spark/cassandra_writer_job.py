
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType


def create_spark_session():
    
    return (SparkSession.builder
        .appName("WikipediaCassandraWriter")
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0")
        .config("spark.cassandra.connection.host", "cassandra")
        .config("spark.cassandra.connection.port", "9042")
        .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.cassandra.output.consistency.level", "ONE")
        .master("local[*]")
        .getOrCreate())


def get_processed_schema():
    
    return StructType([
        StructField("user_id", StringType(), True),
        StructField("domain", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("page_title", StringType(), True)
    ])


if __name__ == "__main__":
    kafka_broker = os.environ.get("KAFKA_BROKER", "kafka:29092")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("Starting Spark Cassandra Writer Job...")
        print(f"Reading from: {kafka_broker}/processed")
        print("Writing to: Cassandra wiki_namespace.edits")

        
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_broker) \
            .option("subscribe", "processed") \
            .option("startingOffsets", "latest") \
            .load()

        
        schema = get_processed_schema()
        parsed_df = df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")

        
        final_df = parsed_df \
            .withColumn("created_at", to_timestamp(col("created_at"))) \
            .filter(col("user_id").isNotNull()) \
            .filter(col("domain").isNotNull()) \
            .filter(col("created_at").isNotNull()) \
            .filter(col("page_title").isNotNull())

        
        query = final_df.writeStream \
            .outputMode("append") \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="edits", keyspace="wiki_namespace") \
            .option("checkpointLocation", "/tmp/spark_checkpoints_cassandra") \
            .option("spark.cassandra.output.ignoreNulls", "true") \
            .start()

        print("Spark Cassandra Writer started successfully!")
        
        query.awaitTermination()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        spark.stop()