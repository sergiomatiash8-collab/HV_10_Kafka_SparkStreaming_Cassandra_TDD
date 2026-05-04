# import pytest
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import udf
# from pyspark.sql.types import StringType
# import uuid
# 
# def test_string_to_uuid_conversion():
#     
#     
#     from src.spark.uuid_utils import string_to_uuid
#     
#     
#     test_id = "550e8400-e29b-41d4-a716-446655440000"
#     
#     
#     result = string_to_uuid(test_id)
#     assert isinstance(result, str)  
#     assert len(result) == 36  
#     
#     
#     invalid_result = string_to_uuid("invalid-uuid")
#     assert invalid_result is None
# 
# def test_uuid_udf_in_spark():
#     
#     from src.spark.uuid_utils import string_to_uuid
#     
#     spark = SparkSession.builder \
#         .appName("UUIDTest") \
#         .master("local[1]") \
#         .getOrCreate()
#     
#     
#     data = [
#         ("550e8400-e29b-41d4-a716-446655440000",),
#         ("invalid-uuid",),
#         (None,)
#     ]
#     df = spark.createDataFrame(data, ["id_string"])
#     
#     
#     uuid_udf = udf(string_to_uuid, StringType())
#     result_df = df.withColumn("id_uuid", uuid_udf(df.id_string))
#     
#     
#     results = result_df.collect()
#     
#     assert results[0].id_uuid is not None  
#     assert results[1].id_uuid is None      
#     assert results[2].id_uuid is None      
#     
#     spark.stop()