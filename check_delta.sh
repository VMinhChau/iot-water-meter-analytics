#!/bin/bash
# Check delta hdfs after batch processing
CONTAINER="spark-master"
DELTA_PATH="hdfs://namenode:8020/data/water_meter/batch/daily_stats"

echo "Running Delta check inside $CONTAINER ..."

docker exec -it $CONTAINER bash -c "
/opt/spark/bin/pyspark --jars /opt/spark/jars/delta-spark_2.12-3.2.0.jar,/opt/spark/jars/delta-storage-3.2.0.jar << EOF
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, min, max

spark = SparkSession.builder \\
    .appName('DeltaCheck') \\
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \\
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \\
    .getOrCreate()

# Load Delta table
df = spark.read.format('delta').load('$DELTA_PATH')

print('=== 10 first rows ===')
df.show(10)

print('=== Schema ===')
df.printSchema()

total_records = df.count()
print('Total records:', total_records)

print('=== Aggregates per meter_id ===')
df.groupBy('meter_id').agg(
    sum('total_value').alias('sum_total_value'),
    avg('total_value').alias('avg_total_value'),
    min('total_value').alias('min_total_value'),
    max('total_value').alias('max_total_value')
).show(10)

spark.stop()
EOF
"