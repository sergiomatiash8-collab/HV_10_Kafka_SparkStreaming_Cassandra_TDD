# tests/unit/test_processor_versions.py
"""
Unit test: Перевірка версій пакетів у processor.py.
Запускається без Docker, без Spark.
"""
import os


def test_processor_uses_consistent_connector_version():
    """
    processor.py має використовувати версії 3.5.0 для всіх Spark пакетів.
    Невідповідність версій → connector не знаходить потрібних JAR.
    """
    # ВИПРАВЛЕННЯ: шлях від кореня проєкту, не відносний
    path = os.path.join(
        os.path.dirname(__file__), '../../src/spark/processor.py'
    )
    path = os.path.normpath(path)
    assert os.path.exists(path), f'processor.py не знайдено: {path}'

    content = open(path, encoding='utf-8').read()

    assert 'spark-cassandra-connector_2.12:3.5.0' in content, \
        'Версія Cassandra connector має бути 3.5.0'
    assert 'spark-sql-kafka-0-10_2.12:3.5.0' in content, \
        'Версія Kafka connector має бути 3.5.0'
    assert '3.4.1' not in content, \
        'Стара версія 3.4.1 знайдена в processor.py!'