import logging
import time
from datetime import datetime
from pyspark.sql import SparkSession
from config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SystemMonitor:
    def __init__(self, config=None):
        self.config = config or Config()
        self.spark = SparkSession.builder \
            .appName("SystemMonitor") \
            .getOrCreate()
    
    def check_data_freshness(self):
        """Check if data is being ingested regularly"""
        try:
            df = self.spark.read.json(self.config.HDFS_RAW_PATH)
            latest_timestamp = df.agg({"timestamp": "max"}).collect()[0][0]
            
            if latest_timestamp:
                latest_time = datetime.fromisoformat(latest_timestamp.replace('Z', '+00:00'))
                time_diff = datetime.now() - latest_time.replace(tzinfo=None)
                
                if time_diff.total_seconds() > 3600:  # 1 hour
                    logger.warning(f"Data is stale. Latest record: {latest_timestamp}")
                    return False
                else:
                    logger.info(f"Data is fresh. Latest record: {latest_timestamp}")
                    return True
            else:
                logger.error("No data found in HDFS")
                return False
                
        except Exception as e:
            logger.error(f"Failed to check data freshness: {e}")
            return False
    
    def check_data_quality(self):
        """Check data quality metrics"""
        try:
            df = self.spark.read.json(self.config.HDFS_RAW_PATH)
            total_records = df.count()
            
            # Check for null values
            null_counts = {}
            for col_name in df.columns:
                null_count = df.filter(df[col_name].isNull()).count()
                null_percentage = (null_count / total_records) * 100
                null_counts[col_name] = null_percentage
                
                if null_percentage > 5:  # More than 5% nulls
                    logger.warning(f"High null percentage in {col_name}: {null_percentage:.2f}%")
            
            # Check for negative values
            negative_values = df.filter(df.value < 0).count()
            if negative_values > 0:
                logger.warning(f"Found {negative_values} negative readings")
            
            logger.info(f"Data quality check completed. Total records: {total_records}")
            return null_counts
            
        except Exception as e:
            logger.error(f"Failed to check data quality: {e}")
            return {}
    
    def check_processing_performance(self):
        """Monitor processing performance"""
        try:
            # Check batch processing output
            daily_stats = self.spark.read.parquet(f"{self.config.HDFS_BATCH_PATH}/daily_stats")
            monthly_stats = self.spark.read.parquet(f"{self.config.HDFS_BATCH_PATH}/monthly_stats")
            problem_meters = self.spark.read.parquet(f"{self.config.HDFS_BATCH_PATH}/problem_meters")
            
            logger.info(f"Daily stats records: {daily_stats.count()}")
            logger.info(f"Monthly stats records: {monthly_stats.count()}")
            logger.info(f"Problem meters: {problem_meters.count()}")
            
            return {
                "daily_stats": daily_stats.count(),
                "monthly_stats": monthly_stats.count(),
                "problem_meters": problem_meters.count()
            }
            
        except Exception as e:
            logger.error(f"Failed to check processing performance: {e}")
            return {}
    
    def generate_health_report(self):
        """Generate comprehensive system health report"""
        logger.info("Generating system health report...")
        
        report = {
            "timestamp": datetime.now().isoformat(),
            "data_freshness": self.check_data_freshness(),
            "data_quality": self.check_data_quality(),
            "processing_performance": self.check_processing_performance()
        }
        
        return report

if __name__ == "__main__":
    monitor = SystemMonitor()
    
    while True:
        try:
            report = monitor.generate_health_report()
            logger.info("Health check completed")
            time.sleep(300)  # Check every 5 minutes
        except KeyboardInterrupt:
            logger.info("Monitoring stopped")
            break
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            time.sleep(60)  # Wait 1 minute before retry















import logging
import time
import uuid

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SpeedLayer")

class SpeedLayer:
    def __init__(self, config: Config = None):
        self.config = config or Config()

        # SparkSession: note you can also pass spark.jars.packages via spark-submit CLI.
        self.spark = (
            SparkSession.builder
            .appName("WaterMeterSpeedLayer")
            # Optional: if you want to enforce ES config at session level
            .config("spark.es.nodes", self.config.ES_HOSTS)  # e.g. "elasticsearch" or "localhost"
            # .config("spark.es.port", str(self.config.ES_PORT))
            .config("spark.es.nodes.wan.only", "true")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
        )

        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("SparkSession created for SpeedLayer")

    def get_schema(self):
        """Return StructType for message JSON payload (adjust types as needed)."""
        return StructType([
            StructField("meter_id", StringType(), True),
            StructField("timestamp", StringType(), True),  # will parse to Timestamp later
            StructField("measurement_type", StringType(), True),
            StructField("series", StringType(), True),
            StructField("unit", StringType(), True),
            StructField("value", DoubleType(), True),
        ])

    def create_kafka_stream(self):
        logger.info("Creating Kafka stream from %s", self.config.KAFKA_BOOTSTRAP_SERVERS)
        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.config.KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", self.config.KAFKA_TOPIC)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

    def process_realtime_stream(self):
        raw_stream = self.create_kafka_stream()

        schema = self.get_schema()

        parsed = raw_stream.select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ).select("data.*", "kafka_timestamp")

        # parse timestamp (assumes ISO-8601 or adjust format)
        parsed = parsed.withColumn("timestamp_parsed", to_timestamp(col("timestamp")))

        # filter invalid/dirty rows early
        filtered = parsed.filter(
            (col("value").isNotNull()) &
            (col("measurement_type").isNotNull()) &
            (col("meter_id").isNotNull()) &
            (col("timestamp_parsed").isNotNull()) &
            (col("value") >= 0) &
            (col("value") <= 1e9)  # sensible upper bound
        )

        # Aggregation: windowed stats
        stats = (
            filtered.filter(col("measurement_type").isin("Pulse1", "Pulse1_Total"))
            .withWatermark("timestamp_parsed", self.config.WATERMARK_DELAY)
            .groupBy(
                window(col("timestamp_parsed"), self.config.WINDOW_DURATION, self.config.SLIDE_DURATION),
                col("meter_id"),
                col("measurement_type")
            )
            .agg(
                sum("value").alias("total_value"),
                avg("value").alias("avg_value"),
                max("value").alias("max_value"),
                min("value").alias("min_value"),
                count("*").alias("reading_count"),
                stddev("value").alias("std_dev"),
                current_timestamp().alias("processed_time")
            )
        )

        # Create a stable id for upsert: meter + window_start (ISO)
        stats = stats.withColumn("window_start", date_format(col("window").start, "yyyy-MM-dd'T'HH:mm:ss"))
        stats = stats.withColumn("doc_id", concat(col("meter_id"), lit("_"), col("measurement_type"), lit("_"), col("window_start")))

        # Anomaly detection: produce records for anomalies (append)
        anomalies = filtered.filter(
            ((col("measurement_type") == "Pulse1") & (col("value") > self.config.HIGH_FLOW_THRESHOLD)) |
            ((col("measurement_type") == "Battery") & (col("value") < self.config.LOW_BATTERY_THRESHOLD)) |
            ((col("measurement_type") == "DeviceTemperature") & (col("value") > self.config.HIGH_TEMP_THRESHOLD))
        ).withColumn("alert_type",
            when((col("measurement_type") == "Pulse1") & (col("value") > self.config.HIGH_FLOW_THRESHOLD), lit("HIGH_FLOW"))
            .when((col("measurement_type") == "Battery") & (col("value") < self.config.LOW_BATTERY_THRESHOLD), lit("LOW_BATTERY"))
            .otherwise(lit("HIGH_TEMPERATURE"))
        ).withColumn("severity",
            when(col("alert_type") == "HIGH_FLOW", lit("WARNING"))
            .when(col("alert_type") == "LOW_BATTERY", lit("CRITICAL"))
            .otherwise(lit("WARNING"))
        ).withColumn("alert_timestamp", current_timestamp())

        # For anomalies, add a unique id to avoid accidental overwrite if you prefer upsert:
        anomalies = anomalies.withColumn("doc_id", concat(col("meter_id"), lit("_"), date_format(col("timestamp_parsed"), "yyyyMMddHHmmss"), lit("_"), expr("uuid()")))

        return stats, anomalies

    def _write_batch_to_es(self, df, epoch_id, index_name, upsert_id_col=None, op_type="index"):
        """
        Write a (micro)batch DataFrame to Elasticsearch.
        - upsert_id_col: if provided, will be used as ES document id (upsert).
        - op_type: "index" (append) or "upsert" (update by id using mapping id).
        """
        if df is None:
            return

        # fast empty check
        if len(df.take(1)) == 0:
            logger.info("Empty micro-batch for %s at epoch %s", index_name, epoch_id)
            return

        # Add helpful metadata
        enriched = df.withColumn("batch_id", lit(str(epoch_id))).withColumn("ingestion_time", current_timestamp())

        # If upsert by id is requested, set mapping id option
        write_builder = enriched.write.format("org.elasticsearch.spark.sql").option("es.nodes", self.config.ES_HOSTS).option("es.port", str(self.config.ES_PORT)).option("es.nodes.wan.only", "true")

        # tuning options (adjust as needed)
        write_builder = write_builder.option("es.batch.size.entries", "1000").option("es.batch.write.retry.count", "3").option("es.batch.write.retry.wait", "30s").option("es.http.timeout", "5m").mode("append")

        if upsert_id_col:
            # tell connector to use this column as document id
            write_builder = write_builder.option("es.mapping.id", upsert_id_col)
            # if you prefer upsert semantics, connector will index by id (append will overwrite same id)
        try:
            write_builder.save(index_name)
            logger.info("Wrote micro-batch to ES index=%s (epoch=%s) rows=%d", index_name, epoch_id, df.count())
        except Exception as e:
            logger.exception("Failed writing batch to ES index=%s: %s", index_name, e)
            # Optionally implement retry logic (exponential backoff) here
            raise

    def start_speed_layer(self):
        stats_df, anomalies_df = self.process_realtime_stream()

        # Stats -> upsert by doc_id into ES index
        stats_query = stats_df.writeStream.foreachBatch(
            lambda df, epoch_id: self._write_batch_to_es(df, epoch_id, self.config.ES_STATS_INDEX, upsert_id_col="doc_id")
        ).outputMode("update").option("checkpointLocation", f"{self.config.CHECKPOINT_LOCATION}/stats").queryName("speed_stats").trigger(processingTime=self.config.TRIGGER_INTERVAL).start()

        # Anomalies -> append into ES index (alerts)
        anomalies_query = anomalies_df.writeStream.foreachBatch(
            lambda df, epoch_id: self._write_batch_to_es(df, epoch_id, self.config.ES_ALERTS_INDEX, upsert_id_col=None)
        ).outputMode("append").option("checkpointLocation", f"{self.config.CHECKPOINT_LOCATION}/anomalies").queryName("speed_anomalies").trigger(processingTime=self.config.TRIGGER_INTERVAL).start()

        logger.info("Speed layer started: stats_query=%s anomalies_query=%s", stats_query.name, anomalies_query.name)
        return [stats_query, anomalies_query]

    def stop_queries(self, queries):
        for q in queries:
            try:
                if q.isActive:
                    q.stop()
            except Exception as e:
                logger.warning("Error stopping query %s: %s", getattr(q, "name", "<unknown>"), e)

    def monitor_queries(self, queries, interval_seconds=60):
        try:
            for q in queries:
                info = q.lastProgress
                logger.info("Query %s active=%s lastProgress=%s", q.name, q.isActive, info)
        except Exception as e:
            logger.warning("Monitoring failed: %s", e)


if __name__ == "__main__":
    conf = Config()
    layer = SpeedLayer(conf)
    queries = []
    try:
        queries = layer.start_speed_layer()
        logger.info("Speed layer running. Press Ctrl+C to stop.")
        while True:
            layer.monitor_queries(queries, interval_seconds=60)
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Shutting down speed layer...")
    finally:
        layer.stop_queries(queries)
        layer.spark.stop()
        logger.info("Shutdown complete.")
