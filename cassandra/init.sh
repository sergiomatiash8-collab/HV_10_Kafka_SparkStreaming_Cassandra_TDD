#!/bin/bash
set -e

echo " Waiting Cassandra..."

until cqlsh cassandra -e "DESCRIBE KEYSPACES" > /dev/null 2>&1; do
  echo "Cassandra not ready, wait 5 sec..."
  sleep 5
done

echo "Cassandra ready"
echo "Processing init.cql..."

cqlsh cassandra -f /init.cql

if [ $? -eq 0 ]; then
    echo " Schema created!"
else
    echo "ERROR schema reating"
    exit 1
fi