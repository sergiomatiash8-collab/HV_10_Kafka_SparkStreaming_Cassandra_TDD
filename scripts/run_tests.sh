
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
# UNIT TESTS (speedy, w/o Docker)
# ============================================

log "Start Unit Tests..."

pytest tests/unit/ -v --tb=short

if [ $? -eq 0 ]; then
    success "Unit tests passed!"
else
    error "Unit tests failed!"
    exit 1
fi

# ============================================
# SMOKE TESTS (need Docker)
# ============================================

log "Checking whether Docker containers are running..."

cd deploy

if ! docker-compose ps | grep -q "Up"; then
    log "The containers haven't started yet; let's start them..."
    ./scripts/run_pipeline.sh
    sleep 30
fi

log "Start Smoke Tests..."

docker-compose exec -T spark-processor pytest tests/smoke/ -v --tb=short

if [ $? -eq 0 ]; then
    success "Smoke tests passed!"
else
    error "Smoke tests failed!"
    exit 1
fi

cd ..

success "ALL TESTS HAVE BEEN PASSED! "