#!/bin/bash
set -e  # Зупиняємось при першій помилці

echo '=================================================='
echo '   HV_10 — TDD Pipeline Runner (Windows/Bash)'
echo '=================================================='

# -- КРОК 1: Юніт-тести (локально у venv) --------------
echo ''
echo '↳ Крок 1: Запуск юніт-тестів...'

# Використовуємо явний шлях до venv, щоб уникнути помилки "command not found"
./venv/Scripts/python.exe -m pytest tests/unit/ -v --tb=short

echo '↳ Юніт-тести пройшли!'

# -- КРОК 2: Запуск Docker Compose --------------------
echo ''
echo '↳ Крок 2: Перезапуск інфраструктури Docker...'
cd deploy

# Очищуємо старі контейнери (якщо вони зависли)
docker-compose down --remove-orphans 2>/dev/null || true

# Піднімаємо все з перезбіркою
docker-compose up -d --build
cd ..

# -- КРОК 3: Очікуємо готовності Kafka -----------------
echo ''
echo '⌛ Крок 3: Очікуємо Kafka (localhost:9092)...'
for i in $(seq 1 30); do
    # Перевірка через /dev/tcp (працює в bash без nc)
    if (echo > /dev/tcp/localhost/9092) >/dev/null 2>&1; then
        echo '↳ Kafka готова!'
        break
    fi
    echo "  Спроба $i/30 (чекаємо 5 сек)..."
    sleep 5
done

# -- КРОК 4: Очікуємо готовності Cassandra -------------
# -- КРОК 4: Очікуємо готовності Cassandra -------------
echo ''
echo '⌛ Крок 4: Очікуємо готовності Cassandra (порт 9042)...'
for i in $(seq 1 30); do
    if (echo > /dev/tcp/localhost/9042) >/dev/null 2>&1; then
        echo '↳ Cassandra відкрила порт 9042!'
        break
    fi
    echo "  Спроба $i/30 (чекаємо 5 сек)..."
    sleep 5
done

# Тепер, коли Кассандра точно працює, перезапускаємо ініціалізатор
echo '↳ Запускаємо скрипт створення таблиць (cassandra-init)...'
cd deploy
docker-compose restart cassandra-init
cd ..

# Даємо скрипту 10 секунд на виконання CQL-запитів
sleep 10

# -- КРОК 5: Smoke-тести (всередині Docker) ------------
echo ''
echo '↳ Крок 5: Запуск smoke-тестів у Spark-контейнері...'

# Запускаємо pytest прямо в робочому контейнері
# -x означає зупинку при першій невдачі
docker exec deploy-spark-processor-1 pytest tests/smoke/ -v --tb=short -x

echo ''
echo '=================================================='
echo '  🎉 Всі тести пройшли! Pipeline успішний. '
echo '=================================================='