import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType

def create_spark_session(kafka_broker):
    """Створює Spark сесію, адаптовану під Docker-мережу."""
    return (SparkSession.builder
        .appName("WikipediaStreamProcessor")
        # Використовуємо версію 3.4.1 для уникнення конфліктів зі Scala 2.12
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1")
        # Оптимізація для локального запуску в контейнері
        .config("spark.sql.shuffle.partitions", "1")
        .master("local[*]")
        .getOrCreate())

if __name__ == "__main__":
    # Отримуємо адресу Kafka з системних змінних Docker (KAFKA_BROKER=kafka:29092)
    kafka_broker = os.environ.get("KAFKA_BROKER", "localhost:9092")
    
    spark = create_spark_session(kafka_broker)
    spark.sparkContext.setLogLevel("ERROR")

    # Схема даних відповідно до Wikipedia 'page-create' стріму
    schema = StructType([
        StructField("id", LongType(), True),
        StructField("page_title", StringType(), True),
        StructField("user_text", StringType(), True),
        StructField("dt", StringType(), True)
    ])

    try:
        print(f"\nSpark підключається до Kafka за адресою: {kafka_broker}")
        
        # 1. Читання стріму з топіка 'input'
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_broker) \
            .option("subscribe", "input") \
            .option("startingOffsets", "latest") \
            .load()

        # 2. Обробка: десеріалізація JSON за схемою
        parsed_df = df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*")

        print("\n" + "="*40)
        print("SPARK PROCESSOR УСПІШНО ЗАПУЩЕНО")
        print("="*40 + "\n")

        # 3. Вивід результату в консоль контейнера
        query = parsed_df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .option("checkpointLocation", "/tmp/spark_checkpoints") \
            .start()

        query.awaitTermination()
        
    except Exception as e:
        print(f"\nКРИТИЧНА ПОМИЛКА SPARK: {e}")
    finally:
        if 'spark' in locals():
            spark.stop()