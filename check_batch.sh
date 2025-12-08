#!/bin/bash
SPARK_MASTER="spark://spark-master:7077"
CONTAINER="spark-master"

# Ensure target directory exists
docker exec $CONTAINER mkdir -p /opt/spark-apps

# Copy the Python script
docker cp check_batch.py $CONTAINER:/opt/spark-apps/check_batch.py

# Run spark-submit using the local JARs
docker exec $CONTAINER /opt/spark/bin/spark-submit \
    --master $SPARK_MASTER \
    --jars /opt/spark/jars/delta-spark_2.12-3.2.0.jar,/opt/spark/jars/delta-storage-3.2.0.jar \
    /opt/spark-apps/check_batch.py