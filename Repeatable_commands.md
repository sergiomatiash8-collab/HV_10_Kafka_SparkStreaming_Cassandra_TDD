docker compose -f deploy/docker-compose.yml up -d
pytest tests/unit/ -v
docker compose -f deploy/docker-compose.yml logs cassandra-init --follow
pytest tests/unit/test_filter_logic.py -v
pytest tests/e2e/test_full_pipeline_e2e.py -v -s