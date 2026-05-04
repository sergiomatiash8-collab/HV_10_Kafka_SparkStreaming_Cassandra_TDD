#!/bin/bash

# ============================================
# Test Orchestration Script
# ============================================

set -e

# Colors for highlighting
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Logging functions
log() {
    echo -e "${BLUE}[TEST]${NC} $1"
}

success() {
    echo -e "${GREEN}✅ $1${NC}"
}

error() {
    echo -e "${RED}❌ $1${NC}"
}

# Store project root to ensure path stability
PROJECT_ROOT=$(pwd)

# ============================================
# UNIT TESTS (Isolated, no Docker required)
# ============================================

log "Starting Unit Tests..."

# Running pytest on the unit directory
pytest tests/unit/ -v --tb=short

if [ $? -eq 0 ]; then
    success "Unit tests passed!"
else
    error "Unit tests failed!"
    exit 1
fi

# ============================================
# SMOKE TESTS (Infrastructure required)
# ============================================

log "Checking infrastructure status..."

# Ensure we are in the deployment directory
cd deploy || error "Directory 'deploy' not found!"

# Check if containers are running, otherwise trigger the run script
if ! docker-compose ps | grep -q "Up"; then
    log "Containers are not running. Initializing pipeline..."
    # Ensure the run script is executed from the correct relative path
    bash ./scripts/run_pipeline.sh || error "Pipeline startup failed!"
    log "Waiting for services to stabilize..."
    sleep 30
fi

log "Starting Smoke Tests inside the container..."

# Execute smoke tests within the Spark Processor container
docker-compose exec -T spark-processor pytest tests/smoke/ -v --tb=short

if [ $? -eq 0 ]; then
    success "Smoke tests passed!"
else
    error "Smoke tests failed!"
    # Return to root before exiting on failure
    cd "$PROJECT_ROOT"
    exit 1
fi

# Return to project root
cd "$PROJECT_ROOT"

echo ""
echo -e "${GREEN}==========================================${NC}"
echo -e "${GREEN}      ALL TESTS PASSED SUCCESSFULLY!      ${NC}"
echo -e "${GREEN}==========================================${NC}"