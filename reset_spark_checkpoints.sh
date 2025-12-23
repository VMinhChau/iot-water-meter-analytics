#!/bin/bash

echo "🔄 Resetting Spark Checkpoints..."
echo "================================"

docker exec -it spark-master bash -c "
    rm -rf /tmp/spark_checkpoints
    echo 'Deleted: /tmp/spark_checkpoints'
"