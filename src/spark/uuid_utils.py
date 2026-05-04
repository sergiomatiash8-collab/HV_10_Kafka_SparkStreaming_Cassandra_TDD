# """
# UUID utility functions for Spark.
# """
# 
# import uuid
# 
# 
# def string_to_uuid(id_string):
#     """
#     Converts string in UUID format.
#     
#     Args:
#         id_string: A string that could be a UUID
#         
#     Returns:
#         A string representing the UUID if valid, or `None` if invalid
#     """
#     if id_string is None:
#         return None
#     
#     try:
#         uuid_obj = uuid.UUID(id_string)
#         return str(uuid_obj)
#     except (ValueError, AttributeError, TypeError):
#         return None