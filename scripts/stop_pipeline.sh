#!/bin/bash

# ============================================
# Stop Script для Wikipedia Pipeline
# ============================================

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log "🛑 Зупинка Wikipedia Pipeline..."

cd deploy

# Показуємо статистику перед зупинкою
log "📊 Фінальна статистика даних:"
docker exec cassandra cqlsh -e "SELECT COUNT(*) FROM wiki_namespace.edits;" 2>/dev/null || true

# Зупинка контейнерів
docker-compose down

success "Всі контейнери зупинені!"

log "💾 Volumes збережені. Для повного очищення: docker-compose down -v"

cd ..