import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType

# Domain layer
from src.spark.logic import transform_wikipedia_event

def create_spark_session(kafka_broker):
    """Infrastructure: Конфігурація для оточення HV_10 з виправленими версіями пакетів."""
    return (SparkSession.builder
        .appName("WikipediaStreamProcessor")
        # Синхронізація версій пакетів до 3.5.0 для повної сумісності зі Spark
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0")
        .config("spark.cassandra.connection.host", "cassandra")
        .config("spark.cassandra.connection.port", "9042")
        .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.cassandra.output.ignoreNulls", "true")
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

    try:
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_broker) \
            .option("subscribe", "input") \
            .option("startingOffsets", "earliest") \
            .load()

        # Трансформація: id залишаємо як StringType. 
        # Cassandra connector сам конвертує рядок у UUID при записі.
        parsed_df = df.select(
            transform_udf(col("value").cast("string")).alias("data")
        ).select("data.*") \
         .withColumn("dt", to_timestamp(col("dt")))

        query = parsed_df.writeStream \
            .outputMode("append") \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="edits", keyspace="wiki_namespace") \
            .option("checkpointLocation", "/tmp/spark_checkpoints") \
            .start()

        query.awaitTermination()
    except Exception as e:
        print(f"Infrastructure Error: {e}")