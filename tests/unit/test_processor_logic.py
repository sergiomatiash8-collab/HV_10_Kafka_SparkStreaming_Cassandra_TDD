import pytest
from src.spark.logic import transform_wikipedia_event

def test_transform_wikipedia_event_real_data():
    """
    Test real data from Wikipedia API
    """
    
    raw_input = (
        '{"meta": {"id": "963a55b3-6ed3-4893-a8c2-501f9e4f193a", "dt": "2026-05-01T13:23:46.789Z"}, '
        '"page_title": "Uzhhorod", "performer": {"user_text": "SuperGrey-bot"}}'
    )
    
    expected = {
        "id": "963a55b3-6ed3-4893-a8c2-501f9e4f193a",
        "page_title": "Uzhhorod",
        "user_text": "SuperGrey-bot",
        "dt": "2026-05-01T13:23:46.789Z"
    }
    
    result = transform_wikipedia_event(raw_input)
    
    assert result == expected

def test_transform_wikipedia_event_invalid_json():
    """Check processing incorrect JSON"""
    assert transform_wikipedia_event('{"invalid":') is None