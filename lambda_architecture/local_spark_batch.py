from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from data_enrichment import SparkDataEnrichment
import logging
import os

logger = logging.getLogger(__name__)

class LocalSparkBatch:
    def __init__(self):
        # Connect to Databricks as remote Spark cluster
        self.spark = SparkSession.builder \
            .appName("WaterMeterBatchProcessing") \
            .config("spark.master", f"databricks://token:{os.getenv('DATABRICKS_TOKEN')}@{os.getenv('DATABRICKS_SERVER_HOSTNAME')}") \
            .config("spark.databricks.cluster.id", os.getenv('DATABRICKS_CLUSTER_ID')) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
            .enableHiveSupport() \
            .getOrCreate()
        
        # Create database if not exists
        self.spark.sql("CREATE DATABASE IF NOT EXISTS water_meter")
        
        # Initialize data enrichment
        self.enricher = SparkDataEnrichment(
            self.spark, 
            "hdfs://namenode:9000/data/water-meter-data/metadata/managedobject_details.csv"
        )
    
    def process_batch_data(self):
        """Process batch data using Databricks as remote Spark cluster"""
        try:
            # Read raw data from HDFS (Databricks reads from your local HDFS)
            raw_df = self.spark.read.parquet("hdfs://namenode:9000/data/water-meter-data/raw/")
            logger.info(f"Read {raw_df.count()} raw records from local HDFS")
            
            # Also create external table pointing to raw data
            self.spark.sql("""
                CREATE TABLE IF NOT EXISTS water_meter.raw_data (
                    meter_id STRING,
                    timestamp TIMESTAMP,
                    flow_rate DOUBLE,
                    total_volume DOUBLE,
                    temperature DOUBLE,
                    pressure DOUBLE,
                    date STRING
                )
                USING PARQUET
                LOCATION 'hdfs://namenode:9000/data/water-meter-data/raw/'
            """)
            
            # Enrich data using Databricks Spark
            enriched_df = self.enricher.enrich_dataframe(raw_df)
            logger.info("Data enrichment completed on Databricks")
            
            # Daily aggregations using Databricks compute
            daily_agg = enriched_df.groupBy("meter_id", "date", "suburb", "usage_type").agg(
                avg("flow_rate").alias("avg_flow"),
                max("total_volume").alias("daily_volume"),
                count("*").alias("readings_count")
            )
            
            # Anomaly detection using Databricks Spark
            window_spec = Window.partitionBy("meter_id")
            anomalies = enriched_df.withColumn(
                "avg_flow", avg("flow_rate").over(window_spec)
            ).withColumn(
                "stddev_flow", stddev("flow_rate").over(window_spec)
            ).withColumn(
                "z_score", (col("flow_rate") - col("avg_flow")) / col("stddev_flow")
            ).filter(abs(col("z_score")) > 2.5)
            
            # Usage trends using Databricks Spark
            trends = enriched_df.withColumn(
                "prev_volume", lag("total_volume").over(
                    Window.partitionBy("meter_id").orderBy("timestamp")
                )
            ).withColumn(
                "daily_usage", col("total_volume") - col("prev_volume")
            )
            
            # Write results to Databricks Hive tables
            daily_agg.write.mode("overwrite").saveAsTable("water_meter.daily_aggregation")
            anomalies.write.mode("overwrite").saveAsTable("water_meter.anomalies")
            trends.write.mode("overwrite").saveAsTable("water_meter.usage_trends")
            
            logger.info("✓ Batch processing completed - Results stored in Databricks Hive")
            
            # Show table info
            self.spark.sql("SHOW TABLES IN water_meter").show()
            
            return {
                'daily_agg': daily_agg.count(),
                'anomalies': anomalies.count(),
                'trends': trends.count()
            }
            
        except Exception as e:
            logger.error(f"Batch processing failed: {e}")
            return None
    
    def suburb_analysis(self):
        """Suburb analysis using Databricks compute"""
        enriched_df = self.spark.read.parquet("hdfs://namenode:9000/data/water-meter-data/raw/")
        enriched_df = self.enricher.enrich_dataframe(enriched_df)
        
        suburb_stats = enriched_df.groupBy("suburb", "usage_type").agg(
            countDistinct("meter_id").alias("meter_count"),
            avg("flow_rate").alias("avg_flow_rate"),
            sum("total_volume").alias("total_consumption")
        ).orderBy(desc("total_consumption"))
        
        # Write results to Databricks Hive table
        suburb_stats.write.mode("overwrite").saveAsTable("water_meter.suburb_analysis")
        
        return suburb_stats.collect()
    
    def run_all_analytics(self):
        """Run all analytics using Databricks as remote compute"""
        logger.info("Starting batch processing on Databricks cluster...")
        
        # Process batch data
        batch_results = self.process_batch_data()
        
        # Suburb analysis
        suburb_results = self.suburb_analysis()
        
        logger.info("✓ All analytics completed on Databricks")
        
        return {
            'batch_processing': batch_results,
            'suburb_analysis': len(suburb_results)
        }

if __name__ == "__main__":
    processor = LocalSparkBatch()
    results = processor.run_all_analytics()
    
    print("Batch Processing Results (using Databricks compute):")
    for analysis, data in results.items():
        print(f"  {analysis}: {data}")