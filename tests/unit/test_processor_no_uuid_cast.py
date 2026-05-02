"""
Unit test: Перевірка що processor НЕ використовує .cast("uuid").
"""

import pytest
import ast
import inspect


def test_processor_does_not_cast_uuid():
    """
    Перевіряє що processor.py НЕ використовує .cast("uuid"),
    бо це не підтримується в PySpark.
    
    Замість цього має використовуватися UUID UDF.
    """
    # Читаємо processor.py
    with open('src/spark/processor.py', 'r') as f:
        code = f.read()
    
    # Перевірка що немає .cast("uuid")
    assert '.cast("uuid")' not in code, \
        "Processor використовує .cast('uuid'), що не працює! Використовуйте uuid_udf замість цього."
    
    # Перевірка що UUID UDF імпортується
    assert 'from src.spark.uuid_utils import string_to_uuid' in code, \
        "UUID utils не імпортується! Додайте: from src.spark.uuid_utils import string_to_uuid"
    
    # Перевірка що створюється UUID UDF
    assert 'uuid_udf' in code or 'string_to_uuid' in code, \
        "UUID UDF не створюється! Додайте: uuid_udf = udf(string_to_uuid, StringType())"
    
    print("✅ Processor правильно використовує UUID UDF!")