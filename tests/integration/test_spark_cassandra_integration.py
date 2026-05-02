"""
Integration-тест: Spark -> Cassandra -> читання назад
Запускати після: docker-compose up -d (з ініціалізованою схемою)
"""
import uuid
import pytest
from datetime import datetime, timezone
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col

CASSANDRA_HOST = 'localhost'
KEYSPACE = 'wiki_namespace'
TABLE = 'edits'

@pytest.fixture(scope='module')
def spark():
    """
    Spark-сесія з Cassandra connector. 
    Налаштування HADOOP_HOME та шляхів береться з conftest.py.
    """
    s = (SparkSession.builder
         .appName('CassandraIntegrationTest')
         .config('spark.jars.packages', 
                 'com.datastax.spark:spark-cassandra-connector_2.12:3.5.0')
         .config('spark.cassandra.connection.host', CASSANDRA_HOST)
         .config('spark.sql.extensions', 
                 'com.datastax.spark.connector.CassandraSparkExtensions')
         # Використовуємо локальну папку для тимчасових даних Spark
         .config("spark.local.dir", "./s_tmp")
         .master('local[2]')
         .getOrCreate())
    
    yield s
    s.stop()


def test_spark_writes_row_to_cassandra(spark):
    """
    Spark записує один рядок у Cassandra.
    Перевіряємо, що запис не кидає виняток.
    """
    test_id = str(uuid.uuid4())
    test_data = [Row(
        id=test_id,
        page_title='Spark Integration Test',
        user_text='integration-bot',
        dt=datetime(2026, 5, 1, 10, 0, 0, tzinfo=timezone.utc)
    )]
    df = spark.createDataFrame(test_data)

    df.write \
        .format('org.apache.spark.sql.cassandra') \
        .options(table=TABLE, keyspace=KEYSPACE) \
        .mode('append') \
        .save()


def test_spark_reads_back_written_row(spark):
    """
    Записуємо рядок і читаємо його назад — перевіряємо цілісність даних.
    """
    test_id = str(uuid.uuid4())
    test_title = f'ReadBack-{test_id[:8]}'

    # 1. Записуємо
    write_df = spark.createDataFrame([Row(
        id=test_id,
        page_title=test_title,
        user_text='readback-bot',
        dt=datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)
    )])
    write_df.write \
        .format('org.apache.spark.sql.cassandra') \
        .options(table=TABLE, keyspace=KEYSPACE) \
        .mode('append').save()

    # 2. Читаємо назад
    read_df = (spark.read
               .format('org.apache.spark.sql.cassandra')
               .options(table=TABLE, keyspace=KEYSPACE)
               .load()
               .filter(col('page_title') == test_title))
    
    rows = read_df.collect()
    assert len(rows) == 1, f'Очікували 1 рядок, отримали {len(rows)}'
    assert rows[0]['user_text'] == 'readback-bot'
    assert rows[0]['id'] == test_id


def test_duplicate_id_is_upserted(spark):
    """
    Перевірка механізму Upsert в Cassandra.
    Другий запис з тим самим ID має оновити існуючий рядок.
    """
    test_id = str(uuid.uuid4())

    for title in ['First Write', 'Second Write (upsert)']:
        spark.createDataFrame([Row(
            id=test_id, page_title=title,
            user_text='upsert-bot',
            dt=datetime(2026, 5, 1, tzinfo=timezone.utc)
        )]).write \
            .format('org.apache.spark.sql.cassandra') \
            .options(table=TABLE, keyspace=KEYSPACE) \
            .mode('append').save()
        
    read_df = (spark.read
               .format('org.apache.spark.sql.cassandra')
               .options(table=TABLE, keyspace=KEYSPACE)
               .load()
               .filter(col('id') == test_id))
    rows = read_df.collect()

    assert len(rows) == 1, 'Cassandra мала зробити upsert, а не створити дублікат'
    assert rows[0]['page_title'] == 'Second Write (upsert)'