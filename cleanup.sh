#!/bin/bash
set -e

echo "Cleaning old data..."

# Correct Kafka container detection (exclude kafka-ui)
KAFKA_CONTAINER=$(docker ps --format "{{.Names}}" | grep "kafka-1$" | head -n 1)

if [ -z "$KAFKA_CONTAINER" ]; then
  echo "No Kafka BROKER container found."
else
  echo "Deleting Kafka topic 'water-meter-readings' from container: $KAFKA_CONTAINER"
  docker exec "$KAFKA_CONTAINER" \
    kafka-topics --bootstrap-server localhost:9092 \
    --delete --topic water-meter-readings || true
fi

echo "Deleting old Elasticsearch index..."
curl -X DELETE "localhost:9200/water-meter-index" || true

echo "Deleting old HDFS data..."
docker exec namenode hdfs dfs -rm -r -f /data/water_meter || true

echo "Cleanup complete."

echo "Check kafka"

docker exec "$KAFKA_CONTAINER" \
  kafka-topics --bootstrap-server localhost:9092 --list