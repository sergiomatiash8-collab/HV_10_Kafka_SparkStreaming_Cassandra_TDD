# 1. Запуск інфраструктури
Write-Host "--- Запуск Docker Compose ---" -ForegroundColor Cyan
docker-compose -f deploy/docker-compose.yml up -d

# 2. Очікування ініціалізації Cassandra
Write-Host "--- Очікування готовності Cassandra та створення таблиць ---" -ForegroundColor Cyan
docker wait deploy-cassandra-init-1

# 3. Запуск смоук-тестів
Write-Host "--- Запуск тестів ---" -ForegroundColor Cyan

Write-Host "Тест 1: Перевірка топіків Kafka..." -ForegroundColor Yellow
docker exec -it deploy-spark-processor-1 pytest -s tests/smoke/test_kafka_topics.py

Write-Host "Тест 2: Перевірка наявності даних у Kafka..." -ForegroundColor Yellow
docker exec -it deploy-spark-processor-1 pytest -s tests/smoke/test_kafka_data.py

Write-Host "--- Готово! ---" -ForegroundColor Green