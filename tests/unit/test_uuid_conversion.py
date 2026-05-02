"""
Unit test: Перевірка конвертації string → UUID.
Спочатку має падати (RED).
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import uuid

def test_string_to_uuid_conversion():
    """
    Тестуємо функцію конвертації string → UUID.
    """
    # Імпортуємо функцію яку ще треба написати
    from src.spark.uuid_utils import string_to_uuid
    
    # Тестові дані
    test_id = "550e8400-e29b-41d4-a716-446655440000"
    
    # Перевірка
    result = string_to_uuid(test_id)
    assert isinstance(result, str)  # UUID буде як string в Spark
    assert len(result) == 36  # Стандартна довжина UUID
    
    # Перевірка невалідного UUID
    invalid_result = string_to_uuid("invalid-uuid")
    assert invalid_result is None

def test_uuid_udf_in_spark():
    """
    Тестуємо UDF для UUID в Spark DataFrame.
    """
    from src.spark.uuid_utils import string_to_uuid
    
    spark = SparkSession.builder \
        .appName("UUIDTest") \
        .master("local[1]") \
        .getOrCreate()
    
    # Створюємо тестові дані
    data = [
        ("550e8400-e29b-41d4-a716-446655440000",),
        ("invalid-uuid",),
        (None,)
    ]
    df = spark.createDataFrame(data, ["id_string"])
    
    # Застосовуємо UDF
    uuid_udf = udf(string_to_uuid, StringType())
    result_df = df.withColumn("id_uuid", uuid_udf(df.id_string))
    
    # Перевірка результатів
    results = result_df.collect()
    
    assert results[0].id_uuid is not None  # Валідний UUID
    assert results[1].id_uuid is None      # Невалідний UUID
    assert results[2].id_uuid is None      # None input
    
    spark.stop()