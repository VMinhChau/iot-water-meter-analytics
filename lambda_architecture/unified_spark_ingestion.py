from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import logging, sys

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("UnifiedSparkIngestion")

DELTA_PATH = "hdfs://namenode:8020/data/water_meter/raw_delta/"
CHECKPOINT_PATH = "hdfs://namenode:8020/data/water_meter/checkpoints/ingestion/"

class UnifiedSparkIngestionStreaming:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("WaterMeterUnifiedIngestionStreaming") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        logger.info("Spark session started successfully with Delta support.")

    def get_schema(self):
        return StructType([
            StructField("meter_id", StringType()),
            StructField("timestamp", StringType()),
            StructField("measurement_type", StringType()),
            StructField("series", StringType()),
            StructField("unit", StringType()),
            StructField("value", DoubleType()),
        ])

    def start_streaming_ingestion(self):
        """Structured Streaming: Kafka → Delta, trigger 2 phút"""
        try:
            df = self.spark.readStream.format("kafka") \
                .option("kafka.bootstrap.servers", "iot-water-meter-analytics-kafka-1:9092") \
                .option("subscribe", "water-meter-readings") \
                .option("startingOffsets", "latest") \
                .load()

            parsed_df = df.select(
                from_json(col("value").cast("string"), self.get_schema()).alias("data")
            ).select("data.*") \
             .withColumn("timestamp", to_timestamp(col("timestamp"))) \
             .withColumn("date", to_date(col("timestamp")))

            query = parsed_df.writeStream \
                .format("delta") \
                .outputMode("append") \
                .option("checkpointLocation", CHECKPOINT_PATH) \
                .option("mergeSchema", "true") \
                .partitionBy("date") \
                .trigger(processingTime="2 minutes") \
                .start(DELTA_PATH)

            logger.info("⏱ Streaming ingestion started (trigger 2 minutes).")
            query.awaitTermination()

        except Exception as e:
            logger.error(f"❌ Streaming ingestion failed: {e}")

if __name__ == "__main__":
    ingestion = UnifiedSparkIngestionStreaming()
    ingestion.start_streaming_ingestion()
