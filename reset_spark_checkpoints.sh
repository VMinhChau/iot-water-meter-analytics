#!/bin/bash

echo "Resetting Spark Streaming Checkpoints (HDFS)"
echo "============================================"

docker exec namenode hdfs dfs -rm -r -f /data/water_meter/checkpoints

echo "Deleted: hdfs:///data/water_meter/checkpoints"