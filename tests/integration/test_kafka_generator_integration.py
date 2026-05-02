"""
Integration-тест: Kafka → Spark Streaming (memory sink)
Memory sink дозволяє перевірити дані без реальної Cassandra.
Запускати після: docker-compose up -d
"""
import json
import time
import uuid
import pytest
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StructType, StructField, StringType
from src.spark.logic import transform_wikipedia_event


KAFKA_BROKER = 'localhost:9092'
TOPIC = 'input'


@pytest.fixture(scope='module')
def spark():
    s = (SparkSession.builder
         .appName('KafkaSparkIntegrationTest')
         .config('spark.jars.packages',
                 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0')
         .config('spark.sql.shuffle.partitions', '1')
         .master('local[2]')
         .getOrCreate())
    yield s
    s.stop()


def test_spark_reads_kafka_stream_and_transforms(spark):
    """
    Надсилаємо повідомлення в Kafka, Spark читає їх у мікробатчі
    і трансформує через transform_wikipedia_event.
    Записуємо в пам'ять (memory sink) і перевіряємо результат.
    """
    logic_schema = StructType([
        StructField('id', StringType(), True),
        StructField('page_title', StringType(), True),
        StructField('user_text', StringType(), True),
        StructField('dt', StringType(), True),
    ])
    transform_udf = udf(transform_wikipedia_event, logic_schema)

    # Запускаємо streaming запит у пам'ять
    raw_df = (spark.readStream
              .format('kafka')
              .option('kafka.bootstrap.servers', KAFKA_BROKER)
              .option('subscribe', TOPIC)
              .option('startingOffsets', 'latest')
              .load())

    parsed_df = (raw_df
                 .select(transform_udf(col('value').cast('string')).alias('d'))
                 .select('d.*'))

    query_name = f'test_stream_{uuid.uuid4().hex[:8]}'
    query = (parsed_df.writeStream
             .outputMode('append')
             .format('memory')
             .queryName(query_name)
             .start())

    # Даємо Spark 3 секунди підписатись на топік
    time.sleep(3)

    # Надсилаємо тестове повідомлення
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    test_title = f'StreamTest-{uuid.uuid4().hex[:6]}'
    producer.send(TOPIC, value={
        'meta': {'id': str(uuid.uuid4()), 'dt': '2026-05-01T10:00:00Z'},
        'page_title': test_title,
        'performer': {'user_text': 'stream-bot'}
    })
    producer.flush()
    producer.close()

    # Чекаємо обробки мікробатчу
    time.sleep(5)
    query.processAllAvailable()

    result = spark.sql(f'SELECT * FROM {query_name} WHERE page_title = \"{test_title}\"')
    rows = result.collect()
    query.stop()

    assert len(rows) >= 1, f'Spark не отримав повідомлення з Kafka (топік: {TOPIC})'
    assert rows[0]['user_text'] == 'stream-bot'