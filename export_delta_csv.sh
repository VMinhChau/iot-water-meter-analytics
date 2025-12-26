#!/bin/bash

# Local output directory for CSVs
LOCAL_OUTPUT_DIR="$PWD/water_meter_csv"
mkdir -p "$LOCAL_OUTPUT_DIR"

# Pre-create folder inside container
docker exec spark-master mkdir -p /tmp/water_meter_csv

# Copy the local Python script into the container
docker cp export_delta_csv.py spark-master:/tmp/export_delta_csv.py

# Run PySpark inside the running spark-master container
docker exec -i spark-master /opt/spark/bin/spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.catalogImplementation=hive \
  /tmp/export_delta_csv.py

# Copy CSVs from container to local current directory
docker cp spark-master:/tmp/water_meter_csv "$LOCAL_OUTPUT_DIR"

echo "All tables exported successfully to $LOCAL_OUTPUT_DIR"