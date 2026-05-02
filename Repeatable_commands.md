docker compose -f deploy/docker-compose.yml up -d
pytest tests/unit/ -v
docker compose -f deploy/docker-compose.yml logs cassandra-init --follow