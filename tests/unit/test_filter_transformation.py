import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from src.spark.filter_job import get_wikipedia_schema

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("LogicTest") \
        .getOrCreate()

def test_filter_logic_transformation(spark):
    allowed_domains = ["en.wikipedia.org", "www.wikidata.org", "commons.wikimedia.org"]
    
    data = [
        Row(value='{"meta": {"id": "1", "dt": "2026-05-01T10:00:00Z", "domain": "en.wikipedia.org"}, "performer": {"user_text": "User1", "user_is_bot": false}, "page_title": "Title1"}'),
        Row(value='{"meta": {"id": "2", "dt": "2026-05-01T10:01:00Z", "domain": "en.wikipedia.org"}, "performer": {"user_text": "Bot1", "user_is_bot": true}, "page_title": "Title2"}'),
        Row(value='{"meta": {"id": "3", "dt": "2026-05-01T10:02:00Z", "domain": "uk.wikipedia.org"}, "performer": {"user_text": "User2", "user_is_bot": false}, "page_title": "Title3"}')
    ]
    
    from pyspark.sql.functions import col, from_json
    df = spark.createDataFrame(data)
    schema = get_wikipedia_schema()
    
    parsed_df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")
    
    filtered_df = parsed_df \
        .filter(col("meta.domain").isin(allowed_domains)) \
        .filter(col("performer.user_is_bot") == False)
    
    results = filtered_df.collect()
    
    assert len(results) == 1
    assert results[0]["meta"]["domain"] == "en.wikipedia.org"
    assert results[0]["performer"]["user_text"] == "User1"