#!/bin/bash

echo "=========================================="
echo " RESETTING KAFKA OFFSETS + CHECKPOINT "
echo "=========================================="

# Group ID used by Spark Structured Streaming
GROUP_ID="spark-kafka-source-5fa7bc33-d970-4944-a164-9446666dcbf4-1270862132-executor"

echo "→ Resetting Kafka offsets to latest..."
docker exec kafka kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --group $GROUP_ID \
  --topic water-meter-readings \
  --reset-offsets --to-latest --execute

echo "→ Deleting ingestion checkpoint from HDFS..."
docker exec namenode hdfs dfs -rm -r /data/water_meter/checkpoints/ingestion

echo "→ Restarting ingestion job..."
docker exec -it spark-master \
  /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:2.4.0 \
  --master spark://spark-master:7077 \
  /opt/spark-apps/lambda_architecture/unified_spark_ingestion.py

echo "=========================================="
echo " INGESTION RESET COMPLETED "
echo "=========================================="