import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType

# Імпортуємо протестовану логіку з нашого модуля
from src.spark.logic import transform_wikipedia_event

def create_spark_session(kafka_broker):
    """Створює Spark сесію з підтримкою Kafka та Cassandra."""
    return (SparkSession.builder
        .appName("WikipediaStreamProcessor")
        # Додаємо обидва пакети: Kafka та Cassandra Connector
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0")
        # Налаштування підключення до Cassandra всередині Docker
        .config("spark.cassandra.connection.host", "cassandra")
        .config("spark.sql.shuffle.partitions", "1")
        .master("local[*]")
        .getOrCreate())

if __name__ == "__main__":
    # Отримуємо адресу Kafka з системних змінних Docker
    kafka_broker = os.environ.get("KAFKA_BROKER", "kafka:29092")
    
    spark = create_spark_session(kafka_broker)
    spark.sparkContext.setLogLevel("ERROR")

    # Схема, яку повертає наша функція трансформації
    logic_schema = StructType([
        StructField("id", StringType(), True),
        StructField("page_title", StringType(), True),
        StructField("user_text", StringType(), True),
        StructField("dt", StringType(), True)
    ])

    # Реєструємо UDF
    transform_udf = udf(transform_wikipedia_event, logic_schema)

    try:
        print(f"\nSpark підключається до Kafka: {kafka_broker} та Cassandra: cassandra")
        
        # 1. Читання стріму з Kafka
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_broker) \
            .option("subscribe", "input") \
            .option("startingOffsets", "latest") \
            .load()

        # 2. Обробка за допомогою TDD-логіки та підготовка типів для Cassandra
        parsed_df = df.select(
            transform_udf(col("value").cast("string")).alias("data")
        ).select("data.*") \
         .withColumn("dt", to_timestamp(col("dt"))) # Конвертуємо в Timestamp для бази

        print("\n" + "="*40)
        print("SPARK PROCESSOR (KAFKA -> CASSANDRA) READY")
        print("="*40 + "\n")

        # 3. Запис результату в Cassandra
        query = parsed_df.writeStream \
            .outputMode("append") \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="edits", keyspace="wiki_namespace") \
            .option("checkpointLocation", "/tmp/spark_cassandra_checkpoints") \
            .start()

        query.awaitTermination()
        
    except Exception as e:
        print(f"\nКРИТИЧНА ПОМИЛКА SPARK: {e}")
    finally:
        if 'spark' in locals():
            spark.stop()