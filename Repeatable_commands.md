docker compose -f deploy/docker-compose.yml up -d
pytest tests/unit/ -v
docker compose -f deploy/docker-compose.yml logs cassandra-init --follow
pytest tests/unit/test_filter_logic.py -v
pytest tests/e2e/test_full_pipeline_e2e.py -v -s

Get-ChildItem -Path . -Exclude "venv",".git",".pytest_cache" | Select-Object Name, @{Name="Summary"; Expression={ if($_.PSIsContainer) { (Get-ChildItem $_.FullName | Select-Object -First 3 Name) -join ", " } else { "" } }}

docker compose -f deploy/docker-compose.yml up -d --build cassandra-init


docker exec -it cassandra cqlsh -e "DESCRIBE TABLE wiki_namespace.edits;"

docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

docker exec -it cassandra cqlsh -e "SELECT count(*) FROM wiki_namespace.edits;"

