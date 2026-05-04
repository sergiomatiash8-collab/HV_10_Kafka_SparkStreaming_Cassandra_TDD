import json
import pytest

def parse_wikipedia_event(raw_data):
    try:
        return json.loads(raw_data)
    except (json.JSONDecodeError, TypeError):
        return None

def test_parse_valid_json():
    raw_event = '{"domain": "commons.wikimedia.org", "user": "PantheraLeo1359531", "bot": false}'
    parsed = parse_wikipedia_event(raw_event)
    
    assert parsed["domain"] == "commons.wikimedia.org"
    assert parsed["user"] == "PantheraLeo1359531"
    assert parsed["bot"] is False

def test_parse_invalid_json():
    assert parse_wikipedia_event("not a json") is None
    assert parse_wikipedia_event(None) is None

def test_parse_missing_fields():
    raw_event = '{"domain": "uk.wikipedia.org"}'
    parsed = parse_wikipedia_event(raw_event)
    
    assert parsed["domain"] == "uk.wikipedia.org"
    assert "user" not in parsed or parsed.get("user") is None