from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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

            raw_df = self.spark.read.format("delta").load(input_path)
            logger.info("Loaded Delta data successfully")

            record_count = raw_df.count()
            if record_count == 0:
                logger.warning(f"No data found in {input_path}")
                return None, None, None

            logger.info(f"Found {record_count} raw records to process")

            # Enrich raw data
            enriched_df = self.enricher.enrich_dataframe(raw_df)

            # Rename columns to be SQL/Tableau friendly
            if 'Meter Type' in enriched_df.columns:
                enriched_df = enriched_df.withColumnRenamed('Meter Type', 'meter_type')
            if 'Usage Type' in enriched_df.columns:
                enriched_df = enriched_df.withColumnRenamed('Usage Type', 'usage_type')
            if 'Suburb' in enriched_df.columns:
                enriched_df = enriched_df.withColumnRenamed('Suburb', 'suburb')

            # Filter & validation
            filtered_df = enriched_df.filter(
                (F.col("value").isNotNull()) &
                (F.col("measurement_type").isNotNull()) &
                (F.col("meter_id").isNotNull()) &
                (F.col("timestamp").isNotNull()) &
                (F.col("value") >= 0) &
                (F.col("value") <= 100000) &
                (F.col("meter_id") > 0) &
                (F.col("measurement_type").isin(["Pulse1", "Pulse1_Total", "Battery", "DeviceTemperature"]))
            ).repartition(200, F.col("meter_id")).persist(StorageLevel.MEMORY_AND_DISK)

            logger.info(f"Enriched and filtered {filtered_df.count()} records")

            # Daily aggregation
            daily_stats = filtered_df \
                .withColumn("date", F.to_date(F.col("timestamp"))) \
                .withColumn("year", F.year(F.col("timestamp"))) \
                .withColumn("month", F.month(F.col("timestamp"))) \
                .groupBy("date", "year", "month", "meter_id", "measurement_type", "meter_type", "suburb", "usage_type") \
                .agg(
                    F.sum(F.col("value")).alias("total_value"),
                    F.avg(F.col("value")).alias("avg_value"),
                    F.max(F.col("value")).alias("max_value"),
                    F.min(F.col("value")).alias("min_value"),
                    F.count("*").alias("reading_count"),
                    F.current_timestamp().alias("processed_time")
                ).persist(StorageLevel.MEMORY_AND_DISK)

            # Anomaly detection
            problem_meters = daily_stats.filter(
                ((F.col("measurement_type") == "Pulse1_Total") & (F.col("total_value") > self.config.HIGH_CONSUMPTION_THRESHOLD)) |
                ((F.col("measurement_type") == "Battery") & (F.col("avg_value") < self.config.LOW_BATTERY_THRESHOLD)) |
                ((F.col("measurement_type") == "DeviceTemperature") & (F.col("max_value") > self.config.HIGH_TEMP_THRESHOLD))
            ).withColumn("issue_type",
                F.when((F.col("measurement_type") == "Pulse1_Total") & (F.col("total_value") > self.config.HIGH_CONSUMPTION_THRESHOLD), "HIGH_CONSUMPTION")
                .when((F.col("measurement_type") == "Battery") & (F.col("avg_value") < self.config.LOW_BATTERY_THRESHOLD), "LOW_BATTERY")
                .otherwise("HIGH_TEMPERATURE")
            ).withColumn("severity",
                F.when(F.col("issue_type") == "HIGH_CONSUMPTION", "WARNING")
                .when(F.col("issue_type") == "LOW_BATTERY", "CRITICAL")
                .otherwise("WARNING")
            ).persist(StorageLevel.MEMORY_AND_DISK)

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
                    F.sum(F.col("total_value")).alias("monthly_total"),
                    F.avg(F.col("avg_value")).alias("monthly_avg"),
                    F.max(F.col("max_value")).alias("monthly_max"),
                    F.count("*").alias("days_active")
                ).persist(StorageLevel.MEMORY_AND_DISK)

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
