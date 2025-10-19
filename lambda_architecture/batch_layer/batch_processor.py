from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import StorageLevel
import logging
import os
import sys

# Add current directory to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from config import Config
from data_enrichment import DataEnrichment
from data_cleaning import DataCleaner
from schema_registry import SchemaRegistry


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BatchProcessor:
    def __init__(self, config=None):
        self.config = config or Config()
        self.enricher = DataEnrichment()
        self.spark = SparkSession.builder \
            .appName("WaterMeterBatchLayer") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        self.cleaner = DataCleaner(self.spark)
        
        # Broadcast meter metadata for efficient joins
        self.meter_metadata_bc = self.spark.sparkContext.broadcast(self.enricher.meter_details)
        logger.info("BatchProcessor initialized successfully")
    
    def process_historical_data(self, input_path=None, output_path=None):
        input_path = input_path or self.config.HDFS_RAW_PATH
        output_path = output_path or self.config.HDFS_BATCH_PATH
        
        try:
            logger.info(f"Processing raw historical data from {input_path}")
            
            # Check if input path exists and has data with better error handling
            try:
                # Use optimized read with schema inference disabled for better performance
                raw_df = self.spark.read \
                    .option("multiline", "true") \
                    .option("inferSchema", "false") \
                    .json(input_path)
                
                record_count = raw_df.count()
                if record_count == 0:
                    logger.warning(f"No data found in {input_path}")
                    return None, None, None
                    
                logger.info(f"Found {record_count} raw records to process")
                
            except FileNotFoundError:
                logger.error(f"Input path not found: {input_path}")
                return None, None, None
            except Exception as e:
                logger.error(f"Failed to read data from {input_path}: {e}")
                raise
            
            # Create metadata DataFrame for joining
            metadata_rows = []
            for meter_id, metadata in self.meter_metadata_bc.value.items():
                metadata_rows.append({
                    'meter_id': meter_id,
                    'meter_type': metadata.get('Meter Type', 'unknown'),
                    'suburb': metadata.get('Suburb', 'unknown'),
                    'postcode': metadata.get('Postcode', 0),
                    'usage_type': metadata.get('Usage Type', 'unknown')
                })
            
            metadata_df = self.spark.createDataFrame(metadata_rows)
            
            # Enrich raw data with metadata via join
            enriched_df = raw_df.join(metadata_df, "meter_id", "left")
            
            # Enhanced data quality filtering with validation and optimization
            filtered_df = enriched_df.filter(
                (col("value").isNotNull()) & 
                (col("measurement_type").isNotNull()) &
                (col("meter_id").isNotNull()) &
                (col("timestamp").isNotNull()) &
                (col("value") >= 0) &
                (col("value") <= 100000) &  # Reasonable upper bound
                (col("meter_id") > 0) &
                (col("measurement_type").isin(["Pulse1", "Pulse1_Total", "Battery", "DeviceTemperature"]))
            ).repartition(200, col("meter_id")).persist(StorageLevel.MEMORY_AND_DISK_SER)  # Optimize partition count
            
            logger.info(f"Enriched and filtered {filtered_df.count()} records")
            
            # Optimized daily aggregations with broadcast join hint
            daily_stats = filtered_df \
                .withColumn("date", to_date(col("timestamp"))) \
                .withColumn("year", year(col("timestamp"))) \
                .withColumn("month", month(col("timestamp"))) \
                .groupBy("date", "year", "month", "meter_id", "measurement_type", "meter_type", "suburb", "usage_type") \
                .agg(
                    sum("value").alias("total_value"),
                    avg("value").alias("avg_value"),
                    max("value").alias("max_value"),
                    min("value").alias("min_value"),
                    count("*").alias("reading_count"),
                    current_timestamp().alias("processed_time")
                ).persist(StorageLevel.MEMORY_AND_DISK_SER)  # Optimized storage level
            
            # Enhanced anomaly detection
            problem_meters = daily_stats.filter(
                ((col("measurement_type") == "Pulse1_Total") & (col("total_value") > self.config.HIGH_CONSUMPTION_THRESHOLD)) |
                ((col("measurement_type") == "Battery") & (col("avg_value") < self.config.LOW_BATTERY_THRESHOLD)) |
                ((col("measurement_type") == "DeviceTemperature") & (col("max_value") > self.config.HIGH_TEMP_THRESHOLD))
            ).withColumn("issue_type",
                when((col("measurement_type") == "Pulse1_Total") & (col("total_value") > self.config.HIGH_CONSUMPTION_THRESHOLD), "HIGH_CONSUMPTION")
                .when((col("measurement_type") == "Battery") & (col("avg_value") < self.config.LOW_BATTERY_THRESHOLD), "LOW_BATTERY")
                .otherwise("HIGH_TEMPERATURE")
            ).withColumn("severity",
                when(col("issue_type") == "HIGH_CONSUMPTION", "WARNING")
                .when(col("issue_type") == "LOW_BATTERY", "CRITICAL")
                .otherwise("WARNING")
            )
            
            # Optimized write with proper file sizing to prevent small files problem
            logger.info("Writing daily stats to HDFS")
            # Calculate optimal partitions based on data size
            record_count = daily_stats.count()
            optimal_partitions = max(1, min(200, record_count // 50000))  # ~50k records per file
            
            daily_stats.coalesce(optimal_partitions).write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .option("parquet.block.size", "134217728")  \
                .option("parquet.page.size", "1048576") \
                .partitionBy("year", "month") \
                .parquet(f"{output_path}/daily_stats")
            
            # Monthly aggregations with suburb-level insights
            monthly_stats = daily_stats \
                .groupBy("year", "month", "meter_id", "measurement_type", "meter_type", "suburb", "usage_type") \
                .agg(
                    sum("total_value").alias("monthly_total"),
                    avg("avg_value").alias("monthly_avg"),
                    max("max_value").alias("monthly_max"),
                    count("*").alias("days_active")
                )
            
            logger.info("Writing monthly stats to HDFS")
            monthly_record_count = monthly_stats.count()
            monthly_partitions = max(1, min(50, monthly_record_count // 10000))
            
            monthly_stats.coalesce(monthly_partitions).write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .option("parquet.block.size", "134217728") \
                .partitionBy("year", "month") \
                .parquet(f"{output_path}/monthly_stats")
            
            # Save problem meters with enhanced metadata
            logger.info("Writing problem meters to HDFS")
            problem_record_count = problem_meters.count()
            problem_partitions = max(1, min(10, problem_record_count // 5000))
            
            problem_meters.coalesce(problem_partitions).write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .option("parquet.block.size", "134217728") \
                .partitionBy("year", "month") \
                .parquet(f"{output_path}/problem_meters")
            
            logger.info("Batch processing completed successfully")
            return daily_stats, monthly_stats, problem_meters
            
        except Exception as e:
            logger.error(f"Batch processing failed: {e}")
            raise
    
    def run_batch_job(self, input_path=None, output_path=None):
        try:
            logger.info("Starting batch processing job")
            daily_stats, monthly_stats, problem_meters = self.process_historical_data(input_path, output_path)
            
            if daily_stats:
                logger.info(f"Processed {daily_stats.count()} daily aggregations")
                logger.info(f"Generated {monthly_stats.count()} monthly aggregations")
                logger.info(f"Identified {problem_meters.count()} problem meters")
            
            return daily_stats, monthly_stats, problem_meters
            
        except Exception as e:
            logger.error(f"Batch job failed: {e}")
            raise
        finally:
            # Clean up cached DataFrames
            try:
                if 'filtered_df' in locals():
                    filtered_df.unpersist()
                if 'daily_stats' in locals():
                    daily_stats.unpersist()
            except Exception as e:
                logger.warning(f"Error during cleanup: {e}")
            
            self.spark.stop()

if __name__ == "__main__":
    try:
        processor = BatchProcessor()
        daily_stats, monthly_stats, problem_meters = processor.run_batch_job()
        logger.info("Batch processing completed successfully")
    except Exception as e:
        logger.error(f"Batch processing failed: {e}")
        exit(1)