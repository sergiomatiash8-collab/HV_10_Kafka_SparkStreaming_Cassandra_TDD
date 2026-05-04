#!/bin/bash

# ============================================
# Pipeline Stop Script
# ============================================

set -e

# Colors for highlighting
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

# Store project root to ensure path stability
PROJECT_ROOT=$(pwd)

log "Stopping Wikipedia Pipeline..."

# Navigate to deployment directory
cd deploy || { echo "Directory 'deploy' not found!"; exit 1; }

# Step 1: Display final stats before shutting down
log "Fetching final data statistics from Cassandra..."
docker exec cassandra cqlsh -e "SELECT COUNT(*) FROM wiki_namespace.edits;" 2>/dev/null || log "Could not retrieve stats (Cassandra might already be down)."

# Step 2: Shut down containers
log "Shutting down all Docker containers..."
docker-compose down

success "All containers have been stopped!"

# Step 3: Inform user about volumes
log "Note: Data volumes have been preserved. To remove them entirely, run: docker-compose down -v"

# Return to project root
cd "$PROJECT_ROOT"