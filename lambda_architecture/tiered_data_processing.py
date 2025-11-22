#!/usr/bin/env python3
"""
Tiered data processing - keep all data but process by priority
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from meter_schema_config import WATER_ANALYTICS_SCHEMA, ANALYTICS_PRIORITIES

class TieredDataProcessor:
    def __init__(self):
        self.spark = SparkSession.builder.appName("TieredProcessing").getOrCreate()
    
    def batch_layer_processing(self, raw_df):
        """Batch layer: Process all data with different frequencies"""
        
        # Hot path: High priority measurements (frequent processing)
        hot_data = raw_df.filter(
            col("series").isin(["P1", "T1", "10266/0", "10266/1", "10267/0"])
        )
        
        # Warm path: Medium priority (daily processing)
        warm_data = raw_df.filter(
            col("series").isin(["V", "T", "10268/0", "10269/0", "10270/0"])
        )
        
        # Cold path: Low priority (weekly processing)
        cold_data = raw_df.filter(
            ~col("series").isin(["P1", "T1", "10266/0", "10266/1", "10267/0", "V", "T", "10268/0", "10269/0", "10270/0"])
        )
        
        return hot_data, warm_data, cold_data
    
    def speed_layer_processing(self, streaming_df):
        """Speed layer: Only critical measurements for real-time"""
        
        # Real-time: Only flow measurements
        realtime_df = streaming_df.filter(
            col("series").isin(["P1", "T1", "10266/1"])  # Flow intervals only
        )
        
        return realtime_df
    
    def storage_strategy(self, hot_data, warm_data, cold_data):
        """Different storage strategies by data temperature"""
        
        # Hot data: Parquet, partitioned by hour
        hot_data.write.mode("append") \
            .partitionBy("year", "month", "day", "hour") \
            .parquet("hdfs://namenode:9000/data/hot/")
        
        # Warm data: Parquet, partitioned by day
        warm_data.write.mode("append") \
            .partitionBy("year", "month", "day") \
            .parquet("hdfs://namenode:9000/data/warm/")
        
        # Cold data: Compressed Parquet, partitioned by month
        cold_data.write.mode("append") \
            .option("compression", "gzip") \
            .partitionBy("year", "month") \
            .parquet("hdfs://namenode:9000/data/cold/")

def create_unified_view():
    """Create unified view combining all data tiers"""
    spark = SparkSession.builder.getOrCreate()
    
    # Union all data tiers for complete analytics
    hot_df = spark.read.parquet("hdfs://namenode:9000/data/hot/")
    warm_df = spark.read.parquet("hdfs://namenode:9000/data/warm/")
    cold_df = spark.read.parquet("hdfs://namenode:9000/data/cold/")
    
    unified_df = hot_df.union(warm_df).union(cold_df)
    
    # Create Hive table for unified access
    unified_df.write.mode("overwrite").saveAsTable("water_analytics.unified_meter_data")
    
    return unified_df

if __name__ == "__main__":
    processor = TieredDataProcessor()
    print("✅ Tiered processing: Keep all data, process by priority")