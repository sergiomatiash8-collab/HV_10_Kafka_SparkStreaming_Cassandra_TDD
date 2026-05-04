import json

def parse_wikipedia_event(raw_data):
    
    return json.loads(raw_data)

def test_parse_valid_json():
    
    raw_event = '{"id": 123, "domain": "en.wikipedia.org", "user_is_bot": false}'
    parsed = parse_wikipedia_event(raw_event)
    
    assert parsed["id"] == 123
    assert parsed["domain"] == "en.wikipedia.org"
    assert parsed["user_is_bot"] is False