import pytest
from src.spark.cassandra_writer_job import get_processed_schema
from src.spark.filter_job import get_wikipedia_schema
from pyspark.sql.types import StructType

def test_wikipedia_input_schema_structure():
    schema = get_wikipedia_schema()
    assert isinstance(schema, StructType)
    field_names = [f.name for f in schema.fields]
    assert "meta" in field_names
    assert "performer" in field_names
    assert "page_title" in field_names

def test_processed_output_schema_structure():
    schema = get_processed_schema()
    assert isinstance(schema, StructType)
    field_names = [f.name for f in schema.fields]
    expected_fields = ["user_id", "domain", "created_at", "page_title"]
    assert all(field in field_names for field in expected_fields)