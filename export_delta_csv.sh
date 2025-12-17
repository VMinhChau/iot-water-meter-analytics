#!/bin/bash
# Export Delta table from HDFS to CSV
CONTAINER="spark-master"
DELTA_PATH="hdfs://namenode:8020/data/water_meter/batch/daily_stats"
CSV_PATH="/opt/spark/work-dir/daily_stats_export.csv"

echo "🚀 Exporting Delta table to CSV inside $CONTAINER ..."

docker exec -it $CONTAINER bash -c "
/opt/spark/bin/pyspark --packages io.delta:delta-core_2.12:2.4.0 << EOF
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName('ExportDeltaCSV') \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .getOrCreate()

# Load Delta table
dt = DeltaTable.forPath(spark, '$DELTA_PATH')
df = dt.toDF()

# Export to CSV
df.coalesce(1).write.option('header', 'true').mode('overwrite').csv('$CSV_PATH')

print(f'✅ Exported CSV to $CSV_PATH')

spark.stop()
EOF
"