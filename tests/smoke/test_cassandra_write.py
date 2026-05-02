import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime
import os
import time

def test_manual_cassandra_write():
    # 1. Глобальний шлях поза проєктом, щоб уникнути конфліктів доступу Windows
    # Створюємо папку C:\spark_tmp, якщо її нема (або інший корінь диска)
    spark_tmp = "C:\\spark_tmp"
    if not os.path.exists(spark_tmp):
        try:
            os.makedirs(spark_tmp, exist_ok=True)
        except:
            spark_tmp = os.path.expanduser("~\\spark_tmp")
            os.makedirs(spark_tmp, exist_ok=True)

    # 2. Налаштування "Випаленої землі"
    # Відключаємо все: UI, серіалізатори, логи, щоб мінімізувати кількість відкритих файлів
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("CassandraDiagnostic") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
        .config("spark.cassandra.connection.host", "127.0.0.1") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
        .config("spark.local.dir", spark_tmp) \
        .config("spark.ui.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.driver.extraJavaOptions", f"-Djava.io.tmpdir={spark_tmp}") \
        .getOrCreate()

    # 3. Схема (мінімальна)
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("page_title", StringType(), True),
        StructField("user_text", StringType(), True),
        StructField("dt", TimestampType(), True)
    ])
    
    data = [("diag-static-001", "Diagnostic Page", "tester", datetime.now())]
    df = spark.createDataFrame(data, schema)

    print(f"\n--- WORKING DIR: {spark_tmp} ---")
    
    try:
        # 4. ЗАПИС (Використовуємо direct join та вимикаємо адаптивне виконання для тесту)
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="edits", keyspace="wiki_namespace") \
            .mode("append") \
            .save()
        print("✅ УСПІХ: Записано!")
        
    except Exception as e:
        print(f"❌ ПОМИЛКА: {str(e)}")
        pytest.fail(f"Fucked up again: {e}")
        
    finally:
        # Важливо: зупиняємо сесію, але не намагаємося видалити файли з Python
        # Windows все одно їх не віддасть, доки JVM не помре повністю
        spark.stop()
        time.sleep(2)