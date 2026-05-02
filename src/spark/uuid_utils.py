"""
UUID utility functions для Spark.
"""

import uuid


def string_to_uuid(id_string):
    """
    Конвертує string в UUID format.
    
    Args:
        id_string: String що може бути UUID
        
    Returns:
        String UUID якщо валідний, None якщо невалідний
    """
    if id_string is None:
        return None
    
    try:
        uuid_obj = uuid.UUID(id_string)
        return str(uuid_obj)
    except (ValueError, AttributeError, TypeError):
        return None