#!/bin/bash

# ============================================
# Test Runner Script
# Запускає тести в правильному порядку (TDD)
# ============================================

set -e

GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

log() {
    echo -e "${BLUE}[TEST]${NC} $1"
}

success() {
    echo -e "${GREEN}✅ $1${NC}"
}

error() {
    echo -e "${RED}❌ $1${NC}"
}

# ============================================
# UNIT TESTS (швидкі, без Docker)
# ============================================

log "Запуск Unit Tests..."

pytest tests/unit/ -v --tb=short

if [ $? -eq 0 ]; then
    success "Unit tests passed!"
else
    error "Unit tests failed!"
    exit 1
fi

# ============================================
# SMOKE TESTS (потребують Docker)
# ============================================

log "Перевірка чи запущені Docker контейнери..."

cd deploy

if ! docker-compose ps | grep -q "Up"; then
    log "Контейнери не запущені, запускаємо..."
    ./scripts/run_pipeline.sh
    sleep 30
fi

log "Запуск Smoke Tests..."

docker-compose exec -T spark-processor pytest tests/smoke/ -v --tb=short

if [ $? -eq 0 ]; then
    success "Smoke tests passed!"
else
    error "Smoke tests failed!"
    exit 1
fi

cd ..

success "ВСІ ТЕСТИ ПРОЙШЛИ! 🎉"