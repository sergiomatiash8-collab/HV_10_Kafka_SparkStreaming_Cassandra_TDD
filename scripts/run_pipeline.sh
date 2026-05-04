#!/bin/bash

# ============================================
# Orchestration Script for Wikipedia Pipeline
# ============================================

set -e 

# Colors for highlighting
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log() { echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"; }
error() { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }
success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARNING]${NC} $1"; }

# ============================================
# Step 1: Check of prerequisites
# ============================================
log "Checking system prerequisites..."
command -v docker &> /dev/null || error "Docker is not installed!"
command -v docker-compose &> /dev/null || error "Docker Compose is not installed!"

# Store project root to ensure path stability
PROJECT_ROOT=$(pwd)

# ============================================
# Step 2: Cleaning of previous sessions
# ============================================
log "Cleaning previous containers and volumes..."
cd deploy || error "Directory 'deploy' not found!"
docker-compose down -v --remove-orphans 2>/dev/null || true
success "Cleanup finalized"

# ============================================
# Step 3: Image building
# ============================================
log "Building Docker images..."
docker-compose build --no-cache
success "Images built successfully"

# ============================================
# Step 4: Infrastructure start
# ============================================
log "Starting infrastructure (Zookeeper, Kafka, Cassandra)..."
docker-compose up -d zookeeper kafka cassandra

log "Waiting for Kafka to be ready..."
sleep 10
until docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 &> /dev/null; do
    log "Kafka is not ready yet, waiting 5 sec..."
    sleep 5
done
success "Kafka is ready!"

log "Waiting for Cassandra to be ready (this may take up to 60s)..."
until docker exec cassandra cqlsh -e "DESCRIBE KEYSPACES" &> /dev/null; do
    log "Cassandra is still booting, waiting 10 sec..."
    sleep 10
done
success "Cassandra is ready!"

# ============================================
# Step 5: Initialization Cassandra Schema
# ============================================
log "Initializing Cassandra schema..."
docker-compose run --rm cassandra-init
success "Schema created successfully!"

# Check keyspace existence
if docker exec cassandra cqlsh -e "DESCRIBE KEYSPACE wiki_namespace" > /dev/null 2>&1; then
    success "Keyspace 'wiki_namespace' confirmed!"
else
    error "Keyspace was not created!"
fi

# ============================================
# Step 6 & 7: Start Pipeline Components
# ============================================
log "Starting Wikipedia Stream Generator and Spark Processor..."
docker-compose up -d generator spark-processor

log "Waiting 20 sec for data processing to initialize..."
sleep 20

# ============================================
# Step 8: System status
# ============================================
log "Current container status:"
docker-compose ps

# ============================================
# Step 9: Data check
# ============================================
log "Checking for ingested data in Cassandra..."

# Extracting record count from CQL output
RECORD_COUNT=$(docker exec cassandra cqlsh -e "SELECT count(*) FROM wiki_namespace.edits;" | grep -E -o '[0-9]+' | head -n 1 || echo "0")

if [ "$RECORD_COUNT" -gt 0 ]; then
    success "Found $RECORD_COUNT records in Cassandra!"
else
    warn "No records found yet. Spark might still be processing the first micro-batch."
fi

# ============================================
# Step 10: Final Instructions
# ============================================
cd "$PROJECT_ROOT"

echo ""
echo -e "${GREEN}==========================================${NC}"
echo -e "${GREEN}   SYSTEM SET UP SUCCESSFULLY!            ${NC}"
echo -e "${GREEN}==========================================${NC}"
echo ""
echo "Monitoring:"
echo "   Generator Logs:  docker-compose -f deploy/docker-compose.yml logs -f generator"
echo "   Processor Logs:  docker-compose -f deploy/docker-compose.yml logs -f spark-processor"
echo ""
echo "Data check:"
echo "   Query: docker exec -it cassandra cqlsh -e \"SELECT * FROM wiki_namespace.edits LIMIT 10;\""
echo ""
echo "Stop Pipeline:"
echo "   docker-compose -f deploy/docker-compose.yml down"
echo ""
echo "=========================================="