def test_cassandra_cql_schema_defined():
    """Перевіряємо що CQL-скрипт існує і містить потрібні елементи."""
    import os
    # Визначаємо шлях до init.cql відносно цього файлу
    cql_path = os.path.join(os.path.dirname(__file__), '../../deploy/init.cql')
    
    # Перевірка наявності файлу
    assert os.path.exists(cql_path), f'init.cql не знайдено за шляхом: {cql_path}'
    
    with open(cql_path, 'r') as f:
        content = f.read()
    
    # Перевірка вмісту схеми
    assert 'wiki_namespace' in content
    assert 'CREATE TABLE' in content
    assert 'edits' in content
    assert 'id' in content
    assert 'UUID' in content.upper()