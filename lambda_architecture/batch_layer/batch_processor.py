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
from data_enrichment import SparkDataEnrichment
from data_cleaning import DataCleaner

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BatchProcessor:
    def __init__(self, config=None):
        self.config = config or Config()
        self.spark = SparkSession.builder \
            .appName("WaterMeterBatchLayer") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")
        self.cleaner = DataCleaner(self.spark)
        self.enricher = SparkDataEnrichment(self.spark)
        logger.info("BatchProcessor initialized successfully with Delta support")

    def process_historical_data(self, input_path=None, output_path=None):
        input_path = input_path or "hdfs://namenode:8020/data/water_meter/raw_delta/"
        output_path = output_path or self.config.HDFS_BATCH_PATH

        try:
            logger.info(f"Processing raw historical data from {input_path}")

            # Read Delta or fallback to JSON
            try:
                raw_df = self.spark.read.format("delta").load(input_path)
                logger.info("Loaded Delta data successfully")
            except Exception as e:
                logger.warning(f"Delta not found or failed to read, fallback to JSON: {e}")
                raw_df = self.spark.read.option("multiline", "true").json(input_path)
                logger.info("Loaded JSON data successfully")

            record_count = raw_df.count()
            if record_count == 0:
                logger.warning(f"No data found in {input_path}")
                return None, None, None

            logger.info(f"Found {record_count} raw records to process")

            # Enrich raw data
            enriched_df = self.enricher.enrich_dataframe(raw_df)

            # Filter & validation
            filtered_df = enriched_df.filter(
                (col("value").isNotNull()) &
                (col("measurement_type").isNotNull()) &
                (col("meter_id").isNotNull()) &
                (col("timestamp").isNotNull()) &
                (col("value") >= 0) &
                (col("value") <= 100000) &
                (col("meter_id") > 0) &
                (col("measurement_type").isin(["Pulse1", "Pulse1_Total", "Battery", "DeviceTemperature"]))
            ).repartition(200, col("meter_id")).persist(StorageLevel.MEMORY_AND_DISK_SER)

            logger.info(f"Enriched and filtered {filtered_df.count()} records")

            # Daily aggregation
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
                ).persist(StorageLevel.MEMORY_AND_DISK_SER)

            # Anomaly detection
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
            ).persist(StorageLevel.MEMORY_AND_DISK_SER)

            # Write daily stats
            record_count = daily_stats.count()
            optimal_partitions = max(1, min(200, record_count // 50000))
            daily_stats.coalesce(optimal_partitions).write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .partitionBy("year", "month") \
                .save(f"{output_path}/daily_stats")

            # Monthly stats
            monthly_stats = daily_stats.groupBy("year", "month", "meter_id", "measurement_type", "meter_type", "suburb", "usage_type") \
                .agg(
                    sum("total_value").alias("monthly_total"),
                    avg("avg_value").alias("monthly_avg"),
                    max("max_value").alias("monthly_max"),
                    count("*").alias("days_active")
                ).persist(StorageLevel.MEMORY_AND_DISK_SER)

            monthly_partitions = max(1, min(50, monthly_stats.count() // 10000))
            monthly_stats.coalesce(monthly_partitions).write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .partitionBy("year", "month") \
                .save(f"{output_path}/monthly_stats")

            # Problem meters
            problem_partitions = max(1, min(10, problem_meters.count() // 5000))
            problem_meters.coalesce(problem_partitions).write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .partitionBy("year", "month") \
                .save(f"{output_path}/problem_meters")

            logger.info("Batch processing completed successfully")

            # Cleanup persisted DataFrames
            filtered_df.unpersist()
            daily_stats.unpersist()
            monthly_stats.unpersist()
            problem_meters.unpersist()

            return daily_stats, monthly_stats, problem_meters

        except Exception as e:
            logger.error(f"Batch processing failed: {e}")
            raise

    def run_batch_job(self, input_path=None, output_path=None):
        try:
            logger.info("Starting batch processing job")
            return self.process_historical_data(input_path, output_path)
        except Exception as e:
            logger.error(f"Batch job failed: {e}")
            raise
        finally:
            self.spark.stop()


if __name__ == "__main__":
    try:
        processor = BatchProcessor()
        daily_stats, monthly_stats, problem_meters = processor.run_batch_job()
        logger.info("Batch processing completed successfully")
    except Exception as e:
        logger.error(f"Batch processing failed: {e}")
        exit(1)
