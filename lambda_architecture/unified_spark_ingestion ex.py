from pyspark.sql import SparkSession
from pyspark.sql.functions import *
# from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import logging, sys, schedule, time

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("UnifiedSparkIngestion")

class UnifiedSparkIngestion:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("WaterMeterUnifiedIngestion") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        logger.info("Spark session started successfully with Delta support.")
    
    def batch_ingestion(self):
        """Batch layer: Kafka to HDFS Delta"""
        try: 
            df = self.spark \
                .read \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "iot-water-meter-analytics-kafka-1:9092") \
                .option("subscribe", "water-meter-readings") \
                .option("startingOffsets", "earliest") \
                .load()
            
            parsed_df = df.select(
                from_json(col("value").cast("string"), self.get_schema()).alias("data")
            ).select("data.*") \
             .withColumn("timestamp", to_timestamp(col("timestamp"))) \
             .withColumn("date", to_date(col("timestamp")))
            
            count = parsed_df.count()
            logger.info(f"✅ Read {count} records from Kafka")

            delta_path = "hdfs://namenode:8020/data/water_meter/raw_delta/"

            # Write to Delta Lake
            parsed_df.coalesce(10).write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .partitionBy("date") \
                .save(delta_path)
            
            logger.info(f"✅ Wrote {count} records to Delta Lake at {delta_path}")
            return True
        except Exception as e:
            logger.error(f"❌ Batch ingestion failed: {e}")
            return False

    def get_schema(self):
        return StructType([
            StructField("meter_id", StringType()),
            StructField("timestamp", StringType()),
            StructField("measurement_type", StringType()),
            StructField("series", StringType()),
            StructField("unit", StringType()),
            StructField("value", DoubleType()),
        ])

if __name__ == "__main__":
    ingestion = UnifiedSparkIngestion()

    # Schedule every 2 minutes
    schedule.every(2).minutes.do(ingestion.batch_ingestion)

    logger.info("⏱ Batch ingestion scheduler started (every 2 minutes).")

    # Keep scheduler alive
    try:
        while True:
            schedule.run_pending()
            time.sleep(10)
    except KeyboardInterrupt:
        logger.info("🛑 Scheduler stopped manually.")
