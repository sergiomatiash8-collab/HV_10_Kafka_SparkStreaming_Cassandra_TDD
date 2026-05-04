
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

log "Stop Wikipedia Pipeline..."

cd deploy


log "Final data statistics:"
docker exec cassandra cqlsh -e "SELECT COUNT(*) FROM wiki_namespace.edits;" 2>/dev/null || true


docker-compose down

success "All containers have been stopped!"

log "The volumes have been saved. To completely clear them: docker-compose down -v"

cd ..