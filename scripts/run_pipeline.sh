#!/bin/bash

# ============================================
# Orchestration Script для Wikipedia Pipeline
# Start system in correct order
# ============================================

set -e  # Stop if error

# Colors for highlighting
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Log function custom
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
# Step 1: Check of prerequisites
# ============================================

log "System condition check..."

if ! command -v docker &> /dev/null; then
    error "Docker not set up!"
fi

if ! command -v docker-compose &> /dev/null; then
    error "Docker Compose not set up!"
fi

success "System condition met"

# ============================================
# Step 2: Cleaning of previous sessions
# ============================================

log "Previous containers cleaning..."

cd deploy
docker-compose down -v 2>/dev/null || true

success "Cleaning finalized"

# ============================================
# Step 3: Image building
# ============================================

log "Docker image building..."

docker-compose build --no-cache

success "Images built"

# ============================================
# Step 4: Infrastructure start
# ============================================

log "Infrastrucure start (Zookeeper, Kafka, Cassandra)..."

docker-compose up -d zookeeper kafka cassandra


log "Waiting for Kafka to be ready..."
sleep 15


until docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 &> /dev/null; do
    log "Kafka is not ready yet, wait 5 sec..."
    sleep 5
done

success "Kafka готова!"


log "Waiting for Cassandra to be ready..."
sleep 20


until docker exec cassandra cqlsh -e "DESCRIBE KEYSPACES" &> /dev/null; do
    log "Cassandra is not ready yet, wait 10 sec..."
    sleep 10
done

success "Cassandra is ready!"

# ============================================
# Step 5: Initialization Cassandra Schema
# ============================================

log "Initialization Cassandra schema..."

docker-compose up cassandra-init

success "Schema created!"


log "Check created schema..."

docker exec cassandra cqlsh -e "DESCRIBE KEYSPACE wiki_namespace" > /dev/null 2>&1

if [ $? -eq 0 ]; then
    success "Keyspace 'wiki_namespace' confirmed!"
else
    error "Keyspace not created!"
fi

# ============================================
# Step 6: Start  Generator
# ============================================

log "Start Wikipedia Stream Generator..."

docker-compose up -d generator

sleep 5


log "Check generator logs..."
docker-compose logs generator | tail -n 5

success "Generator started!"

# ============================================
# Step 7: Start Spark Processor
# ============================================

log "Start Spark Streaming Processor..."

docker-compose up -d spark-processor

sleep 10


log "Check log processor..."
docker-compose logs spark-processor | tail -n 10

success "Spark Processor started!"

# ============================================
# Step 8: System status
# ============================================

log "Containers current status:"
docker-compose ps

# ============================================
# Step 9: Data check
# ============================================

log "Waiting for 30 sec for data to be collected..."
sleep 30

log "Check data in Cassandra..."

RECORD_COUNT=$(docker exec cassandra cqlsh -e "SELECT COUNT(*) FROM wiki_namespace.edits;" | grep -oP '\d+' | tail -1)

if [ "$RECORD_COUNT" -gt 0 ]; then
    success "In Cassandra found $RECORD_COUNT records!"
else
    warn "In Cassandra still no records, perhaps need to wait..."
fi

# ============================================
# Step 10: User instruction
# ============================================

echo ""
echo "=========================================="
echo -e "${GREEN}System set up successfully!${NC}"
echo "=========================================="
echo ""
echo "Monitoring:"
echo "   Logs Generator:  docker-compose logs -f generator"
echo "   Logs Processor:  docker-compose logs -f spark-processor"
echo "   All logs:        docker-compose logs -f"
echo ""
echo "Data check:"
echo "   Cassandra CLI:   docker exec -it cassandra cqlsh"
echo "   Query:           SELECT * FROM wiki_namespace.edits LIMIT 10;"
echo ""
echo "Stop:"
echo "   ./scripts/stop_pipeline.sh"
echo ""
echo "=========================================="

cd ..