#!/bin/bash

# ============================================
# Cassandra Init Script Wrapper
# Чекає поки Cassandra буде ready, потім виконує CQL
# ============================================

set -e

echo "⏳ Очікування запуску Cassandra..."

# Чекаємо поки Cassandra стане доступною
until cqlsh cassandra -e "DESCRIBE KEYSPACES" > /dev/null 2>&1; do
  echo "Cassandra ще не готова, чекаємо 5 секунд..."
  sleep 5
done

echo "✅ Cassandra готова!"
echo "📝 Виконуємо init.cql..."

# Виконуємо CQL скрипт
cqlsh cassandra -f /init.cql

if [ $? -eq 0 ]; then
    echo "✅ Schema створена успішно!"
else
    echo "❌ Помилка створення schema"
    exit 1
fi