# tests/smoke/test_cassandra_connection.py
import os
import sys
import uuid
import pytest
from pathlib import Path
from datetime import datetime, timezone


# ── Hadoop setup ДО імпорту pyspark ──────────────────────────────────────────
def _setup_hadoop():
    project_root = Path(__file__).parent.parent.parent.resolve()
    hadoop_home = project_root / "hadoop"
    if (hadoop_home / "bin" / "winutils.exe").exists():
        os.environ.setdefault("HADOOP_HOME", str(hadoop_home))
        os.environ.setdefault("hadoop.home.dir", str(hadoop_home))
        bin_path = str(hadoop_home / "bin")
        if bin_path not in os.environ.get("PATH", ""):
            os.environ["PATH"] = bin_path + os.pathsep + os.environ["PATH"]

_setup_hadoop()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


# ── Fixture ───────────────────────────────────────────────────────────────────
@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder
        .appName("SmokeTest-SparkCassandra")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0",
        )
        .config("spark.cassandra.connection.host", "127.0.0.1")
        .config("spark.cassandra.connection.port", "9042")
        .config("spark.sql.extensions",
                "com.datastax.spark.connector.CassandraSparkExtensions")
        .config("spark.sql.catalog.cassandra",
                "com.datastax.spark.connector.datasource.CassandraCatalog")
        .config("spark.cassandra.connection.timeout_ms", "15000")
        .config("spark.ui.enabled", "false")
        # КРИТИЧНО для Windows: вимикаємо Python-серіалізацію при записі.
        # Connector записує напряму через Java — Python worker не запускається.
        .config("spark.cassandra.output.batch.size.rows", "1")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .master("local[1]")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


# ── Тест 1: SparkSession живий ────────────────────────────────────────────────
def test_spark_session_is_alive(spark):
    assert spark is not None
    assert spark.version.startswith("3.5"), f"Версія: {spark.version}"


# ── Тест 2: keyspace і таблиця через Spark SQL ───────────────────────────────
def test_cassandra_namespace_visible(spark):
    """
    Перевіряємо що Spark бачить wiki_namespace через CassandraCatalog.
    Не робимо CREATE — keyspace вже має існувати після cassandra-init.
    """
    result = spark.sql("SHOW NAMESPACES IN cassandra").collect()
    namespaces = [r[0] for r in result]
    assert "wiki_namespace" in namespaces, (
        f"Keyspace 'wiki_namespace' не знайдено!\n"
        f"Доступні: {namespaces}\n"
        f"Запусти: docker exec deploy-cassandra-1 cqlsh -f /init.cql"
    )


# ── Тест 3: таблиця читається ────────────────────────────────────────────────
def test_cassandra_table_schema(spark):
    """Перевіряємо схему через Spark — без cassandra-driver."""
    df = (
        spark.read
        .format("org.apache.spark.sql.cassandra")
        .options(table="edits", keyspace="wiki_namespace")
        .load()
    )
    field_names = [f.name for f in df.schema.fields]
    print(f"\nСхема: {df.schema}")  # видно при pytest -s

    assert "id"         in field_names, f"Немає поля 'id'. Поля: {field_names}"
    assert "page_title" in field_names
    assert "user_text"  in field_names
    assert "dt"         in field_names


# ── Тест 4: запис через Spark SQL (без Python worker!) ───────────────────────
def test_spark_writes_via_sql(spark):
    """
    КЛЮЧОВЕ ВИПРАВЛЕННЯ:
    
    Замість df.write (який запускає Python worker на кожній партиції
    → WinError 10038 на Windows), використовуємо spark.sql() INSERT.
    
    spark.sql INSERT INTO через CassandraCatalog виконується повністю
    на рівні JVM — Python worker НЕ запускається → краш зникає.
    """
    global TEST_WRITE_ID
    TEST_WRITE_ID = str(uuid.uuid4())

    # Реєструємо як temp view — дані вже в JVM після createDataFrame
    data = [(
        TEST_WRITE_ID,
        "Smoke Test Page",
        "smoke-test-bot",
        datetime(2026, 5, 2, 12, 0, 0, tzinfo=timezone.utc),
    )]
    schema = StructType([
        StructField("id",         StringType(),    False),
        StructField("page_title", StringType(),    True),
        StructField("user_text",  StringType(),    True),
        StructField("dt",         TimestampType(), True),
    ])
    df = spark.createDataFrame(data, schema=schema)
    df.createOrReplaceTempView("smoke_input")

    # INSERT через SQL — повністю JVM, без Python serialization
    spark.sql("""
        INSERT INTO cassandra.wiki_namespace.edits
        SELECT
            CAST(id AS STRING) AS id,
            page_title,
            user_text,
            dt
        FROM smoke_input
    """)


# ── Тест 5: читання назад ────────────────────────────────────────────────────
def test_spark_reads_back_written_row(spark):
    """
    Читаємо через SQL — так само через JVM, без Python worker.
    Фільтруємо по page_title бо id є UUID і порівняння складніше.
    """
    result = spark.sql("""
        SELECT id, page_title, user_text
        FROM cassandra.wiki_namespace.edits
        WHERE page_title = 'Smoke Test Page'
        ALLOW FILTERING
    """).collect()

    # Якщо ALLOW FILTERING не спрацює — fallback через connector API
    if not result:
        from pyspark.sql.functions import col
        result = (
            spark.read
            .format("org.apache.spark.sql.cassandra")
            .options(table="edits", keyspace="wiki_namespace")
            .load()
            .filter(col("page_title") == "Smoke Test Page")
            .collect()
        )

    assert len(result) >= 1, (
        "Рядок 'Smoke Test Page' не знайдено після запису.\n"
        "Перевір test_spark_writes_via_sql — він має пройти першим."
    )
    assert result[0]["user_text"] == "smoke-test-bot"
    print(f"\nЗнайдено рядок: {dict(result[0].asDict())}")