from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging, sys, schedule, time

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]  # ensures visibility in Docker logs
)
logger = logging.getLogger("UnifiedSparkIngestion")

class UnifiedSparkIngestion:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("WaterMeterUnifiedIngestion") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        logger.info("Spark session started successfully.")
    
    def batch_ingestion(self):
        """Batch layer: Kafka to HDFS"""
        try: 
            df = self.spark \
                .read \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "iot-water-meter-analytics-kafka-1:9092") \
                .option("subscribe", "water-meter-readings") \
                .option("startingOffsets", "latest") \
                .load()
            
            parsed_df = df.select(
                from_json(col("value").cast("string"), self.get_schema()).alias("data")
            ).select("data.*").withColumn("date", to_date(col("timestamp")))
            
            count = parsed_df.count()
            logger.info(f"✅ Read {count} records from Kafka")

            parsed_df.write \
                .mode("append") \
                .partitionBy("date") \
                .parquet("hdfs://namenode:8020/data/water_meter/raw/")
            
            logger.info(f"✅ Wrote {count} records to HDFS")
            return True
        except Exception as e:
            logger.error(f"❌ Batch ingestion failed: {e}")
            return False
    
    # def streaming_ingestion(self):
    #     """Speed layer: Real-time processing"""
    #     df = self.spark \
    #         .readStream \
    #         .format("kafka") \
    #         .option("kafka.bootstrap.servers", "iot-water-meter-analytics-kafka-1:9092") \
    #         .option("subscribe", "water-meter-readings") \
    #         .load()
        
    #     parsed_df = df.select(
    #         from_json(col("value").cast("string"), self.get_schema()).alias("data")
    #     ).select("data.*")
        
    #     es_query = parsed_df.writeStream \
    #         .outputMode("append") \
    #         .format("org.elasticsearch.spark.sql") \
    #         .option("es.resource", "water-meter-realtime") \
    #         .option("es.nodes", "elasticsearch") \
    #         .option("es.port", "9200") \
    #         .option("checkpointLocation", "/tmp/es-checkpoint") \
    #         .trigger(processingTime='10 seconds') \
    #         .start()
        
    #     logger.info("🚀 Streaming ingestion to Elasticsearch started")
    #     return es_query
    
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
    
    # batch_success = ingestion.batch_ingestion()

    # Schedule every 2 minutes (you can change it)
    schedule.every(2).minutes.do(ingestion.batch_ingestion)

    logger.info("⏱ Batch ingestion scheduler started (every 2 minutes).")

    # Keep scheduler alive
    try:
        while True:
            schedule.run_pending()
            time.sleep(10)  # check every 10 seconds
    except KeyboardInterrupt:
        logger.info("🛑 Scheduler stopped manually.")
    
    # es_query = ingestion.streaming_ingestion()
    # try:
    #     es_query.awaitTermination()
    # except KeyboardInterrupt:
    #     es_query.stop()
    #     logger.info("Streaming ingestion stopped by user.")
