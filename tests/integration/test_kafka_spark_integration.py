import json
import time
import uuid
import pytest
import os
import shutil
import urllib.request
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Налаштування
KAFKA_BROKER = '127.0.0.1:9092'
TOPIC = 'input'

def setup_windows_hadoop():
    """
    АВТОМАТИЧНЕ РІШЕННЯ ДЛЯ WINDOWS:
    Створює локальне середовище Hadoop та завантажує необхідні бінарники (winutils.exe),
    без яких Spark падає на Windows при роботі з файловою системою.
    """
    hadoop_home = os.path.abspath("./hadoop_env").replace("\\", "/")
    bin_dir = f"{hadoop_home}/bin"
    os.makedirs(bin_dir, exist_ok=True)
    os.makedirs(f"{hadoop_home}/tmp", exist_ok=True)
    
    # Офіційні бінарники Hadoop 3.3.5 для Windows (підходять для Spark 3.5)
    winutils_url = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin/winutils.exe"
    hadoop_dll_url = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin/hadoop.dll"
    
    winutils_path = f"{bin_dir}/winutils.exe"
    hadoop_dll_path = f"{bin_dir}/hadoop.dll"
    
    if not os.path.exists(winutils_path):
        print("\n[SETUP] Завантаження winutils.exe для Windows...")
        urllib.request.urlretrieve(winutils_url, winutils_path)
    if not os.path.exists(hadoop_dll_path):
        print("[SETUP] Завантаження hadoop.dll для Windows...")
        urllib.request.urlretrieve(hadoop_dll_url, hadoop_dll_path)

    # Примусово вказуємо Spark використовувати нашу правильну папку
    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["PATH"] += os.pathsep + bin_dir
    return hadoop_home

@pytest.fixture(scope='module')
def spark():
    # 1. Готуємо середовище (завантажить файли при першому запуску)
    hadoop_dir = setup_windows_hadoop()
    
    packages = [
        'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0',
        'org.apache.commons:commons-pool2:2.11.1'
    ]

    # 2. Стартуємо Spark
    s = (SparkSession.builder
         .appName('KafkaSparkIntegration_WinFixed')
         .config('spark.jars.packages', ",".join(packages))
         .config('spark.sql.shuffle.partitions', '1')
         .config('spark.driver.host', '127.0.0.1')
         .config('spark.driver.bindAddress', '127.0.0.1')
         # Направляємо всі темпові файли в нашу контрольовану папку
         .config("spark.local.dir", f"{hadoop_dir}/tmp")
         .master('local[1]')
         .getOrCreate())
    
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()

def test_spark_reads_kafka_stream(spark):
    test_id = f"test_{uuid.uuid4().hex[:6]}"
    query_name = f"query_{test_id}"
    
    # Шлях для чекпоїнтів у нашій стабільній директорії
    hadoop_dir = os.environ["HADOOP_HOME"]
    checkpoint_dir = f"file:///{hadoop_dir}/checkpoints/{test_id}"

    # Читання з Kafka
    raw_df = (spark.readStream
              .format('kafka')
              .option('kafka.bootstrap.servers', KAFKA_BROKER)
              .option('subscribe', TOPIC)
              .option('startingOffsets', 'latest')
              .load()
              .select(col("value").cast("string")))

    query = None
    try:
        # Старт стріму (Тут він раніше падав через файлову систему)
        query = (raw_df.writeStream
                  .outputMode('append')
                  .format('memory')
                  .queryName(query_name)
                  .option("checkpointLocation", checkpoint_dir)
                  .start())

        print(f"\n[INFO] Stream started. Waiting for warm-up...")
        time.sleep(10)

        # Відправка даних
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        producer.send(TOPIC, value={"key": test_id})
        producer.flush()
        producer.close()
        print(f"[INFO] Sent message with ID: {test_id}")

        # Перевірка
        found = False
        for i in range(15):
            time.sleep(1)
            try:
                result = spark.sql(f"SELECT * FROM {query_name}").collect()
                if any(test_id in str(row) for row in result):
                    found = True
                    print(f"✅ УРА! Spark побачив дані на {i+1} секунді.")
                    break
            except Exception as e:
                continue
        
        assert found, "Потік запущено, але дані з Kafka не дійшли до пам'яті."

    finally:
        if query:
            query.stop()