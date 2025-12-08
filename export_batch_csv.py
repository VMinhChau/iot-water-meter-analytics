#!/usr/bin/env python3
from pyspark.sql import SparkSession

# Initialize Spark session with Delta support
spark = SparkSession.builder \
    .appName("ExportBatchToCSV") \
    .getOrCreate()

# Path to your batch-processed Delta table
delta_path = "hdfs://namenode:8020/data/water_meter/batch/daily_stats"

# Load the Delta table
df = spark.read.format("delta").load(delta_path)

# Optional: show schema and sample data
df.printSchema()
df.show(5, truncate=False)

# Write to CSV with header, single file
output_path = "hdfs://namenode:8020/data/water_meter/batch/daily_stats_csv"
df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

print(f"✅ Batch data exported to CSV at {output_path}")

spark.stop()