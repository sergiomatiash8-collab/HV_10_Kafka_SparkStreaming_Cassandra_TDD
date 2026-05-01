import requests

def test_wikipedia_stream_is_reachable():
    """Перевірка, чи доступний API ендпойнт Wikipedia з User-Agent"""
    url = "https://stream.wikimedia.org/v2/stream/page-create"
    # Додаємо заголовок, щоб сервер нас не блокував
    headers = {'User-Agent': 'MyDataPipelineProject/1.0 (contact: your_email@example.com)'}
    
    response = requests.get(url, stream=True, timeout=5, headers=headers)
    
    assert response.status_code == 200, f"Ендпойнт недоступний, статус: {response.status_code}"
    response.close()