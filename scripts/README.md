Wikipedia Real-Time Data Pipeline (Kafka-Spark-Cassandra)
Проєкт з побудови масштабованого пайплайну обробки даних у реальному часі, що використовує потік подій Wikipedia. Система розроблена за методологією TDD (Test-Driven Development) та повністю контейнеризована.

Архітектура системи
Generator (Python): Підключається до Wikipedia EventStreams (SSE) та відправляє події в Kafka.

Kafka & Zookeeper: Виступають як брокер повідомлень та буфер.

Spark Processor (PySpark): Виконує Structured Streaming обробку, трансформацію та валідацію даних.

Cassandra: Кінцеве сховище для оброблених даних.

Швидкий запуск (Windows/Linux)
Для запуску всієї інфраструктури та пайплайну використовуйте оркестраційний скрипт:

Bash
# Надання прав на виконання
chmod +x scripts/run_pipeline.sh

# Запуск системи
./scripts/run_pipeline.sh
Скрипт автоматично перевірить вимоги, побудує Docker-образи, ініціалізує схему Cassandra та запустить потік даних.

Тестування (TDD)
Проєкт має високе покриття тестами, включаючи Unit, Integration та End-to-End рівні.

Запуск тестів:

PowerShell
pytest tests/ -v
Ключові перевірки:

test_wikipedia_to_kafka_e2e: Перевірка доставки подій з Wikipedia до Kafka.

test_full_pipeline_e2e: Повна перевірка шляху Kafka -> Spark -> Cassandra.

Моніторинг та перевірка даних
Після запуску ви можете перевірити кількість оброблених записів у Cassandra:

Bash
docker exec -it cassandra cqlsh -e "SELECT count(*) FROM wiki_namespace.edits;"
Зупинка системи
Для коректного завершення роботи всіх сервісів:

Bash
./scripts/stop_pipeline.sh
Тобі щось додати чи змінити?
Наприклад, я можу додати розділ про технологічний стек (Python 3.11, PySpark 3.x і т.д.) або детальнішу інструкцію з налаштування Virtual Environment.