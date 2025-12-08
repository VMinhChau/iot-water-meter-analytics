#!/bin/bash
echo "🚀 Starting Spark ThriftServer with Delta support..."

# Enter the container if not already inside
# docker exec -it spark-master bash

/opt/spark/sbin/start-thriftserver.sh \
    --master spark://spark-master:7077 \
    --driver-memory 4G \
    --executor-memory 4G \
    --conf spark.sql.catalogImplementation=hive \
    --conf spark.sql.warehouse.dir=/user/hive/warehouse \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --jars /opt/spark/jars/delta-spark_2.12-2.4.0.jar,/opt/spark/jars/hive-jdbc-standalone-3.1.2.jar \
    --verbose