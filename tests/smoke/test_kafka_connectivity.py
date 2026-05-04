import pytest

def test_spark_can_fetch_kafka_metadata(spark):
    """
    Перевіряємо, чи може Spark реально з'єднатися з Kafka брокером 
    і отримати схему (метадані) топіку 'input'.
    """
    try:
        # Спробуємо прочитати лише один батч метаданих
        df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
            .option("subscribe", "input") \
            .load()
        
        # Якщо ми отримали колонки (key, value, topic і т.д.), значить конект є
        assert "value" in df.columns
        assert "topic" in df.columns
        print("\n[SUCCESS] Spark successfully connected to Kafka and retrieved metadata.")
        
    except Exception as e:
        pytest.fail(f"Spark-Kafka connectivity metadata check failed: {e}")