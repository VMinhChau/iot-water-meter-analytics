# check_batch.py
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder \
    .appName('CheckBatch') \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')

spark = configure_spark_with_delta_pip(builder).getOrCreate()

tables = {
    'daily_stats': 'hdfs://namenode:8020/data/water_meter/batch/daily_stats',
    'monthly_stats': 'hdfs://namenode:8020/data/water_meter/batch/monthly_stats',
    'problem_meters': 'hdfs://namenode:8020/data/water_meter/batch/problem_meters'
}

for name, path in tables.items():
    print(f'\n=== {name.upper()} ===')
    try:
        df = spark.read.format('delta').load(path)
        print(f'Total records: {df.count()}')
        df.show(5, truncate=False)
        df.printSchema()
    except Exception as e:
        print(f'Error reading {name}: {e}')

spark.stop()