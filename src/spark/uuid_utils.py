"""
UUID utility functions для Spark.
Конвертує string в UUID format, перевіряючи валідність.
"""

import uuid

def string_to_uuid(id_string):
    """
    Конвертує string в UUID, перевіряючи валідність.
    
    Args:
        id_string: String що може бути UUID
        
    Returns:
        String UUID якщо валідний, None якщо невалідний або None
        
    Examples:
        >>> string_to_uuid("550e8400-e29b-41d4-a716-446655440000")
        '550e8400-e29b-41d4-a716-446655440000'
        
        >>> string_to_uuid("invalid")
        None
    """
    if id_string is None:
        return None
    
    try:
        # Спроба parse як UUID
        uuid_obj = uuid.UUID(id_string)
        # Повертаємо як string (Cassandra приймає UUID як string)
        return str(uuid_obj)
    except (ValueError, AttributeError, TypeError):
        return None