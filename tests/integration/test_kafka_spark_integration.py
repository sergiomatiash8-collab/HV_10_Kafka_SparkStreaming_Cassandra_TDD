import json
import time
import uuid
import pytest
import os
import shutil
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Налаштування
KAFKA_BROKER = '127.0.0.1:9092'
TOPIC = 'input'

@pytest.fixture(scope='module')
def spark():
    """
    Максимально стабільна конфігурація Spark для Windows.
    """
    # Створюємо папку в корені проекту
    tmp_path = os.path.abspath("./spark_tmp").replace("\\", "/")
    if not os.path.exists(tmp_path):
        os.makedirs(tmp_path)

    # Додаємо commons-pool2 до пакетів - це часто вирішує помилки аналізу Kafka
    packages = [
        'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0',
        'org.apache.commons:commons-pool2:2.11.1'
    ]

    s = (SparkSession.builder
         .appName('KafkaSparkIntegrationTest')
         .config('spark.jars.packages', ",".join(packages))
         .config('spark.sql.shuffle.partitions', '1')
         .config('spark.driver.host', '127.0.0.1')
         .config('spark.driver.bindAddress', '127.0.0.1')
         .config("spark.hadoop.fs.permissions.umask-mode", "000")
         # Вказуємо Spark використовувати нашу локальну папку для всього
         .config("spark.sql.streaming.checkpointLocation", f"{tmp_path}/checkpoints")
         .master('local[1]')
         .getOrCreate())
    
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
    
    # Спроба очистити після себе
    time.sleep(2)
    shutil.rmtree(tmp_path, ignore_errors=True)

def test_spark_reads_kafka_stream(spark):
    """
    Спрощений тест для перевірки самого каналу зв'язку.
    """
    test_id = uuid.uuid4().hex[:8]
    checkpoint_path = os.path.abspath(f"./spark_tmp/cp_{test_id}").replace("\\", "/")
    query_name = f"query_{test_id}"

    # 1. Читаємо "сирі" дані без складної логіки
    raw_df = (spark.readStream
              .format('kafka')
              .option('kafka.bootstrap.servers', KAFKA_BROKER)
              .option('subscribe', TOPIC)
              .option('startingOffsets', 'earliest')
              .load()
              .select(col("value").cast("string"))) # Тільки каст у рядок

    query = None
    try:
        # 2. Запуск
        query = (raw_df.writeStream
                  .outputMode('append')
                  .format('memory')
                  .queryName(query_name)
                  .option("checkpointLocation", checkpoint_path)
                  .start())

        # Даємо час на розгортання
        time.sleep(5)

        # 3. Відправка повідомлення
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        test_message = {"test_val": test_id}
        producer.send(TOPIC, value=test_message)
        producer.flush()
        producer.close()

        # 4. Очікування результату
        found = False
        print(f"\n[TEST] Looking for {test_id}...")
        for _ in range(10):
            time.sleep(2)
            # Перевіряємо чи є дані в таблиці
            result = spark.sql(f"SELECT * FROM {query_name}").collect()
            if any(test_id in str(row) for row in result):
                found = True
                print(f"✅ Found data in Spark memory!")
                break
        
        assert found, "Spark не отримав дані з Kafka."

    finally:
        if query:
            query.stop()