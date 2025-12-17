#!/bin/bash
set -e

# Auto-detect spark master container
CONTAINER=$(docker ps --format "{{.Names}}" | grep -E "spark-master" | head -n 1)

if [ -z "$CONTAINER" ]; then
  echo "Spark Master container not found! Start with: docker compose up -d"
  exit 1
fi

APP_PATH="/opt/spark-apps/lambda_architecture/batch_layer/batch_processor.py"

echo "Running Batch Processor with spark-submit inside $CONTAINER ..."
docker exec -it "$CONTAINER" /opt/spark/bin/spark-submit \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --jars /opt/spark/jars/delta-spark_2.12-3.2.0.jar,/opt/spark/jars/delta-storage-3.2.0.jar \
  "$APP_PATH"