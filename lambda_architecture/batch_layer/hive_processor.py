from pyspark.sql import SparkSession
from pyspark.sql.functions import *

class HiveProcessor:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("WaterMeterHiveProcessor") \
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
            .enableHiveSupport() \
            .getOrCreate()
    
    def create_hive_tables(self):
        # Create database
        self.spark.sql("CREATE DATABASE IF NOT EXISTS water_meter")
        self.spark.sql("USE water_meter")
        
        # Raw data table
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS raw_readings (
                meter_id BIGINT,
                timestamp TIMESTAMP,
                measurement_type STRING,
                series STRING,
                unit STRING,
                value DOUBLE,
                meter_type STRING,
                suburb STRING,
                postcode INT,
                usage_type STRING
            )
            STORED AS PARQUET
            LOCATION '/data/water_meter/raw'
        """)
        
        # Daily aggregations table
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS daily_stats (
                meter_id BIGINT,
                date DATE,
                measurement_type STRING,
                total_value DOUBLE,
                avg_value DOUBLE,
                max_value DOUBLE,
                min_value DOUBLE,
                reading_count BIGINT
            )
            STORED AS PARQUET
            LOCATION '/data/water_meter/batch_views/daily_stats'
        """)
        
        # Monthly aggregations table
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS monthly_stats (
                meter_id BIGINT,
                month STRING,
                measurement_type STRING,
                monthly_total DOUBLE,
                monthly_avg DOUBLE
            )
            STORED AS PARQUET
            LOCATION '/data/water_meter/batch_views/monthly_stats'
        """)
        
        # Problem meters table
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS problem_meters (
                meter_id BIGINT,
                date DATE,
                measurement_type STRING,
                total_value DOUBLE,
                avg_value DOUBLE,
                max_value DOUBLE,
                min_value DOUBLE,
                reading_count BIGINT,
                issue_type STRING
            )
            STORED AS PARQUET
            LOCATION '/data/water_meter/batch_views/problem_meters'
        """)
    
    def process_to_hive(self):
        # Read enriched JSON data from HDFS
        df = self.spark.read.json("/data/water_meter/raw")
        
        # Create daily aggregations
        daily_stats = df \
            .withColumn("date", to_date(col("timestamp"))) \
            .groupBy("date", "meter_id", "measurement_type") \
            .agg(
                sum("value").alias("total_value"),
                avg("value").alias("avg_value"),
                max("value").alias("max_value"),
                min("value").alias("min_value"),
                count("*").alias("reading_count")
            )
        
        # Write to Hive table
        daily_stats.write \
            .mode("overwrite") \
            .insertInto("water_meter.daily_stats")
        
        # Monthly aggregations
        monthly_stats = daily_stats \
            .withColumn("month", date_format(col("date"), "yyyy-MM")) \
            .groupBy("month", "meter_id", "measurement_type") \
            .agg(
                sum("total_value").alias("monthly_total"),
                avg("avg_value").alias("monthly_avg")
            )
        
        monthly_stats.write \
            .mode("overwrite") \
            .insertInto("water_meter.monthly_stats")
        
        # Process problem meters if provided
        try:
            problem_df = self.spark.read.parquet("/data/water_meter/batch_views/problem_meters")
            problem_df.write \
                .mode("overwrite") \
                .insertInto("water_meter.problem_meters")
        except:
            print("No problem meters data found")
        
        return daily_stats, monthly_stats

if __name__ == "__main__":
    processor = HiveProcessor()
    processor.create_hive_tables()
    processor.process_to_hive()
    print("Data processed to Hive tables")