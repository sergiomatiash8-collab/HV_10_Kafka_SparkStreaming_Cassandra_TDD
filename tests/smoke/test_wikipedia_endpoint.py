import requests

def test_wikipedia_stream_is_reachable():
    
    url = "https://stream.wikimedia.org/v2/stream/page-create"
    
    headers = {'User-Agent': 'MyDataPipelineProject/1.0 (contact: your_email@example.com)'}
    
    response = requests.get(url, stream=True, timeout=5, headers=headers)
    
    assert response.status_code == 200, f"Endpoint unavailable, status: {response.status_code}"
    response.close()