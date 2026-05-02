#!/bin/bash

# ============================================
# Orchestration Script для Wikipedia Pipeline
# Запускає всю систему в правильному порядку
# ============================================

set -e  # Зупинка при помилці

# Кольори для виводу
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Функція для красивого логування
log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
    exit 1
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# ============================================
# КРОК 1: Перевірка prerequisites
# ============================================

log "📋 Перевірка системних вимог..."

if ! command -v docker &> /dev/null; then
    error "Docker не встановлений!"
fi

if ! command -v docker-compose &> /dev/null; then
    error "Docker Compose не встановлений!"
fi

success "Всі системні вимоги виконані"

# ============================================
# КРОК 2: Очищення попередніх запусків
# ============================================

log "🧹 Очищення попередніх контейнерів..."

cd deploy
docker-compose down -v 2>/dev/null || true

success "Очищення завершено"

# ============================================
# КРОК 3: Білд образів
# ============================================

log "🔨 Білд Docker образів..."

docker-compose build --no-cache

success "Образи побудовані"

# ============================================
# КРОК 4: Запуск інфраструктури
# ============================================

log "🚀 Запуск інфраструктури (Zookeeper, Kafka, Cassandra)..."

docker-compose up -d zookeeper kafka cassandra

# Чекаємо Kafka
log "⏳ Очікування готовності Kafka..."
sleep 15

# Перевірка Kafka
until docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 &> /dev/null; do
    log "Kafka ще не готова, чекаємо 5 секунд..."
    sleep 5
done

success "Kafka готова!"

# Чекаємо Cassandra
log "⏳ Очікування готовності Cassandra..."
sleep 20

# Перевірка Cassandra
until docker exec cassandra cqlsh -e "DESCRIBE KEYSPACES" &> /dev/null; do
    log "Cassandra ще не готова, чекаємо 10 секунд..."
    sleep 10
done

success "Cassandra готова!"

# ============================================
# КРОК 5: Ініціалізація Cassandra Schema
# ============================================

log "📝 Ініціалізація Cassandra schema..."

docker-compose up cassandra-init

success "Schema створена!"

# Перевірка schema
log "🔍 Перевірка створеної schema..."

docker exec cassandra cqlsh -e "DESCRIBE KEYSPACE wiki_namespace" > /dev/null 2>&1

if [ $? -eq 0 ]; then
    success "Keyspace 'wiki_namespace' підтверджено!"
else
    error "Keyspace не створився!"
fi

# ============================================
# КРОК 6: Запуск Generator
# ============================================

log "📡 Запуск Wikipedia Stream Generator..."

docker-compose up -d generator

sleep 5

# Перевірка логів generator
log "Перевірка логів generator..."
docker-compose logs generator | tail -n 5

success "Generator запущено!"

# ============================================
# КРОК 7: Запуск Spark Processor
# ============================================

log "⚡ Запуск Spark Streaming Processor..."

docker-compose up -d spark-processor

sleep 10

# Перевірка логів processor
log "Перевірка логів processor..."
docker-compose logs spark-processor | tail -n 10

success "Spark Processor запущено!"

# ============================================
# КРОК 8: Статус системи
# ============================================

log "📊 Поточний статус контейнерів:"
docker-compose ps

# ============================================
# КРОК 9: Перевірка даних
# ============================================

log "⏳ Чекаємо 30 секунд для накопичення даних..."
sleep 30

log "🔍 Перевірка даних в Cassandra..."

RECORD_COUNT=$(docker exec cassandra cqlsh -e "SELECT COUNT(*) FROM wiki_namespace.edits;" | grep -oP '\d+' | tail -1)

if [ "$RECORD_COUNT" -gt 0 ]; then
    success "В Cassandra знайдено $RECORD_COUNT записів!"
else
    warn "В Cassandra поки немає записів, можливо треба почекати ще..."
fi

# ============================================
# КРОК 10: Інструкції користувачу
# ============================================

echo ""
echo "=========================================="
echo -e "${GREEN}✅ СИСТЕМА УСПІШНО ЗАПУЩЕНА!${NC}"
echo "=========================================="
echo ""
echo "📊 Моніторинг:"
echo "   Логи Generator:  docker-compose logs -f generator"
echo "   Логи Processor:  docker-compose logs -f spark-processor"
echo "   Всі логи:        docker-compose logs -f"
echo ""
echo "🔍 Перевірка даних:"
echo "   Cassandra CLI:   docker exec -it cassandra cqlsh"
echo "   Query:           SELECT * FROM wiki_namespace.edits LIMIT 10;"
echo ""
echo "🛑 Зупинка:"
echo "   ./scripts/stop_pipeline.sh"
echo ""
echo "=========================================="

cd ..