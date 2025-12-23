from pyspark.sql import SparkSession
import os

OUTPUT_DIR = "/tmp/water_meter_csv"
os.makedirs(OUTPUT_DIR, exist_ok=True)

spark = SparkSession.builder \
    .appName("DeltaToCSV") \
    .getOrCreate()

# List of Delta paths to export
tables = {
    "daily_stats_v2": "hdfs://namenode:8020/data/water_meter/batch/daily_stats_v2",
    "monthly_stats": "hdfs://namenode:8020/data/water_meter/batch/monthly_stats",
    "problem_meters": "hdfs://namenode:8020/data/water_meter/batch/problem_meters"
}

for table_name, path in tables.items():
    print(f"Exporting {table_name} from {path} ...")
    df = spark.read.format("delta").load(path)
    df.write.mode("overwrite").option("header", True).csv(os.path.join(OUTPUT_DIR, table_name))

spark.stop()