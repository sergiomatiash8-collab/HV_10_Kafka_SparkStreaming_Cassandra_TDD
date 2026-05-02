#!/bin/bash
set -e

echo "⏳ Очікування Cassandra..."

until cqlsh cassandra -e "DESCRIBE KEYSPACES" > /dev/null 2>&1; do
  echo "Cassandra не готова, чекаємо 5 сек..."
  sleep 5
done

echo "✅ Cassandra готова!"
echo "📝 Виконуємо init.cql..."

cqlsh cassandra -f /init.cql

if [ $? -eq 0 ]; then
    echo "✅ Schema створена!"
else
    echo "❌ Помилка створення schema"
    exit 1
fi