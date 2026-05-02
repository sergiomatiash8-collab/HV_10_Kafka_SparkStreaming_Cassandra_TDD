"""
Unit test: Перевірка консистентності версій Spark Cassandra Connector.
"""

import pytest
import re


def test_processor_uses_consistent_connector_version():
    """
    Перевіряє що всюди використовується одна версія connector (3.5.0).
    
    Файли для перевірки:
    - src/spark/processor.py
    - deploy/docker-compose.yml
    - tests/unit/test_cassandra_connection.py
    - tests/smoke/test_cassandra_write.py
    """
    expected_version = "3.5.0"
    
    files_to_check = [
        'src/spark/processor.py',
        'deploy/docker-compose.yml',
        'tests/unit/test_cassandra_connection.py',
        'tests/smoke/test_cassandra_write.py'
    ]
    
    version_pattern = r'spark-cassandra-connector_2\.12:(\d+\.\d+\.\d+)'
    
    versions_found = {}
    
    for file_path in files_to_check:
        try:
            with open(file_path, 'r') as f:
                content = f.read()
            
            matches = re.findall(version_pattern, content)
            
            if matches:
                versions_found[file_path] = matches
                
                for version in matches:
                    assert version == expected_version, \
                        f"❌ {file_path} використовує версію {version}, очікувалось {expected_version}!"
        
        except FileNotFoundError:
            pytest.skip(f"Файл {file_path} не знайдено")
    
    print(f"✅ Всі файли використовують connector версії {expected_version}!")
    print(f"Перевірено: {list(versions_found.keys())}")