import json
import time
import uuid
import pytest
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StructType, StructField, StringType

# Імпортуємо вашу бізнес-логіку трансформації
from src.spark.logic import transform_wikipedia_event

KAFKA_BROKER = 'localhost:9092'
TOPIC = 'input'

@pytest.fixture(scope='module')
def spark():
    """Ініціалізація Spark сесії з підтримкою Kafka для тестів."""
    s = (SparkSession.builder
         .appName('KafkaSparkIntegrationTest')
         .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0')
         .config('spark.sql.shuffle.partitions', '1')
         .master('local[2]')
         .getOrCreate())
    yield s
    s.stop()

def test_spark_reads_kafka_stream_and_transforms(spark):
    """
    Основний тест: Kafka -> Spark -> Memory Sink.
    """
    # 1. Описуємо схему для UDF (результат трансформації)
    logic_schema = StructType([
        StructField('id', StringType(), True),
        StructField('page_title', StringType(), True),
        StructField('user_text', StringType(), True),
        StructField('dt', StringType(), True),
    ])
    
    transform_udf = udf(transform_wikipedia_event, logic_schema)

    # 2. Налаштовуємо стрім з Kafka
    raw_df = (spark.readStream
              .format('kafka')
              .option('kafka.bootstrap.servers', KAFKA_BROKER)
              .option('subscribe', TOPIC)
              .option('startingOffsets', 'latest')
              .load())

    # 3. Застосовуємо трансформацію
    parsed_df = (raw_df
                 .select(transform_udf(col('value').cast('string')).alias('d'))
                 .select('d.*'))

    # 4. Запускаємо запит у Memory Sink (тимчасова таблиця в RAM)
    query_name = f'test_stream_{uuid.uuid4().hex[:8]}'
    query = (parsed_df.writeStream
             .outputMode('append')
             .format('memory')
             .queryName(query_name)
             .start())

    # Даємо Spark трохи часу на ініціалізацію підписки
    time.sleep(3)

    # 5. Генеруємо та надсилаємо тестове повідомлення в Kafka
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    test_title = f'StreamTest-{uuid.uuid4().hex[:6]}'
    test_payload = {
        'meta': {'id': str(uuid.uuid4()), 'dt': '2026-05-01T10:00:00Z'},
        'page_title': test_title,
        'performer': {'user_text': 'stream-bot'}
    }
    
    producer.send(TOPIC, value=test_payload)
    producer.flush()
    producer.close()

    # 6. Очікуємо обробки мікробатчу
    time.sleep(5)
    query.processAllAvailable()

    # 7. Перевірка результату через SQL запит до Memory Sink
    result = spark.sql(f"SELECT * FROM {query_name} WHERE page_title = '{test_title}'")
    rows = result.collect()
    
    query.stop()

    # Assertions
    assert len(rows) >= 1, f"Spark не зміг вичитати дані з Kafka топіку '{TOPIC}'"
    assert rows[0]['user_text'] == 'stream-bot'