#!/bin/bash
# Check delta hdfs after batch processing
# Variables
CONTAINER="spark-master"
DELTA_PATH="hdfs://namenode:8020/data/water_meter/batch/daily_stats"

echo "🚀 Running Delta check inside $CONTAINER ..."

docker exec -it $CONTAINER bash -c "
/opt/spark/bin/pyspark --packages io.delta:delta-core_2.12:2.4.0 << 'EOF'
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import sum, avg, min, max

spark = SparkSession.builder \
    .appName('DeltaCheck') \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .getOrCreate()

# Load Delta table using full HDFS URI
dt = DeltaTable.forPath(spark, '$DELTA_PATH')
df = dt.toDF()

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