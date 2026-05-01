import socket

def test_kafka_port_open():
    """Перевірка, чи відкритий порт Kafka (9092)"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex(('127.0.0.1', 9092))
    sock.close()
    assert result == 0, "Порт Kafka 9092 недоступний. Перевір docker-compose!"