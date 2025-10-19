from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import StorageLevel
import logging
import time
from config import Config
from schema_registry import SchemaRegistry


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SpeedLayer:
    def __init__(self, config=None):
        self.config = config or Config()
        self.cached_dataframes = []  # Track cached DataFrames for cleanup
        
        self.spark = SparkSession.builder \
            .appName("WaterMeterSpeedLayer") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.executor.memoryFraction", "0.8") \
            .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("SpeedLayer initialized successfully")
    
    def create_kafka_stream(self):
        try:
            logger.info(f"Creating Kafka stream from {self.config.KAFKA_BOOTSTRAP_SERVERS}")
            return self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.config.KAFKA_BOOTSTRAP_SERVERS) \
                .option("subscribe", self.config.KAFKA_TOPIC) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()
        except Exception as e:
            logger.error(f"Failed to create Kafka stream: {e}")
            raise
    
    def process_realtime_stream(self):
        try:
            raw_stream = self.create_kafka_stream()
            
            # Parse JSON from Kafka using schema registry
            schema = SchemaRegistry.get_schema("iot_raw")
            parsed_stream = raw_stream.select(
                from_json(col("value").cast("string"), schema).alias("data"),
                col("timestamp").alias("kafka_timestamp")
            ).select("data.*", "kafka_timestamp")
            
            # Enhanced data quality filtering
            filtered_stream = parsed_stream.filter(
                (col("value").isNotNull()) & 
                (col("measurement_type").isNotNull()) &
                (col("meter_id").isNotNull()) &
                (col("timestamp").isNotNull()) &
                (col("value") >= 0) &  # No negative readings
                (col("value") <= 100000) &  # Reasonable upper bound
                (col("meter_id") > 0)  # Valid meter ID
            ).persist(StorageLevel.MEMORY_AND_DISK_SER)  # Optimized storage level
            
            # Track for cleanup
            self.cached_dataframes.append(filtered_stream)
            
            # Optimized real-time aggregations with partitioning
            flow_data = filtered_stream.filter(
                col("measurement_type").isin(["Pulse1", "Pulse1_Total"])
            ).repartition(col("meter_id"))  # Partition by meter_id for better performance
            
            sampled_stats = flow_data \
                .withColumn("timestamp_parsed", to_timestamp(col("timestamp"))) \
                .withWatermark("timestamp_parsed", self.config.WATERMARK_DELAY) \
                .groupBy(
                    window(col("timestamp_parsed"), self.config.WINDOW_DURATION, self.config.SLIDE_DURATION),
                    col("meter_id"),
                    col("measurement_type")
                ) \
                .agg(
                    sum("value").alias("total_value"),
                    avg("value").alias("avg_value"),
                    max("value").alias("max_value"),
                    min("value").alias("min_value"),
                    count("*").alias("reading_count"),
                    current_timestamp().alias("processed_time"),
                    stddev("value").alias("std_dev")  # Add standard deviation for anomaly detection
                )
            
            # Enhanced anomaly detection with configurable thresholds
            anomalies = filtered_stream.filter(
                ((col("measurement_type") == "Pulse1") & (col("value") > self.config.HIGH_FLOW_THRESHOLD)) |
                ((col("measurement_type") == "Battery") & (col("value") < self.config.LOW_BATTERY_THRESHOLD)) |
                ((col("measurement_type") == "DeviceTemperature") & (col("value") > self.config.HIGH_TEMP_THRESHOLD))
            ).withColumn("alert_type", 
                when((col("measurement_type") == "Pulse1") & (col("value") > self.config.HIGH_FLOW_THRESHOLD), "HIGH_FLOW")
                .when((col("measurement_type") == "Battery") & (col("value") < self.config.LOW_BATTERY_THRESHOLD), "LOW_BATTERY")
                .otherwise("HIGH_TEMPERATURE")
            ).withColumn("severity", 
                when(col("alert_type") == "HIGH_FLOW", "WARNING")
                .when(col("alert_type") == "LOW_BATTERY", "CRITICAL")
                .otherwise("WARNING")
            ).withColumn("alert_timestamp", current_timestamp())
            
            logger.info("Stream processing pipeline created successfully")
            return sampled_stats, anomalies
            
        except ValueError as e:
            logger.error(f"Invalid data format in stream: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to create stream processing pipeline: {e}")
            # Return empty DataFrames for graceful degradation
            empty_df = self.spark.createDataFrame([], StructType([]))
            return empty_df, empty_df
    
    def write_to_elasticsearch(self, df, epoch_id):
        max_retries = 3
        retry_count = 0
        enriched_df = None
        
        try:
            while retry_count < max_retries:
                try:
                    logger.info(f"Writing batch {epoch_id} to Elasticsearch (attempt {retry_count + 1})")
                    
                    # Validate DataFrame before writing
                    if df.count() == 0:
                        logger.warning(f"Empty DataFrame for batch {epoch_id}, skipping")
                        return
                    
                    # Add metadata and data quality metrics
                    enriched_df = df.withColumn("batch_id", lit(epoch_id)) \
                                   .withColumn("ingestion_time", current_timestamp()) \
                                   .withColumn("data_quality_score", 
                                             when(col("reading_count") > 0, lit(1.0)).otherwise(lit(0.0)))
                    
                    # Write with optimized settings and connection pooling
                    enriched_df.coalesce(1).write \
                        .format("org.elasticsearch.spark.sql") \
                        .option("es.resource", self.config.ES_SPEED_INDEX) \
                        .option("es.nodes", self.config.ES_HOSTS) \
                        .option("es.nodes.wan.only", "true") \
                        .option("es.write.operation", "upsert") \
                        .option("es.mapping.id", "meter_id") \
                        .option("es.batch.size.entries", "1000") \
                        .option("es.batch.write.retry.count", "3") \
                        .option("es.batch.write.retry.wait", "30s") \
                        .option("es.http.timeout", "5m") \
                        .option("es.http.retries", "3") \
                        .mode("append") \
                        .save()
                        
                    logger.info(f"Successfully wrote batch {epoch_id} to Elasticsearch")
                    return
                    
                except ConnectionError as e:
                    retry_count += 1
                    logger.warning(f"Connection error writing batch {epoch_id}: {e}. Retry {retry_count}/{max_retries}")
                    if retry_count < max_retries:
                        time.sleep(2 ** retry_count)  # Exponential backoff
                except Exception as e:
                    logger.error(f"Failed to write batch {epoch_id} to Elasticsearch: {e}")
                    break
            
            if retry_count >= max_retries:
                logger.error(f"Failed to write batch {epoch_id} after {max_retries} attempts")
                
        finally:
            # Clean up memory
            if enriched_df is not None:
                enriched_df.unpersist()
    
    def start_speed_layer(self):
        try:
            sampled_stats, anomalies = self.process_realtime_stream()
            
            # Write aggregated stats to Elasticsearch
            stats_query = sampled_stats.writeStream \
                .foreachBatch(self.write_to_elasticsearch) \
                .outputMode("update") \
                .trigger(processingTime=self.config.TRIGGER_INTERVAL) \
                .option("checkpointLocation", f"{self.config.CHECKPOINT_LOCATION}/stats") \
                .queryName("speed_layer_stats") \
                .start()
            
            # Write anomalies to Kafka alerts topic
            alerts_query = anomalies.select(
                to_json(struct("*")).alias("value")
            ).writeStream \
                .outputMode("append") \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.config.KAFKA_BOOTSTRAP_SERVERS) \
                .option("topic", self.config.KAFKA_ALERTS_TOPIC) \
                .option("checkpointLocation", f"{self.config.CHECKPOINT_LOCATION}/alerts") \
                .queryName("speed_layer_alerts") \
                .start()
            
            logger.info("Speed layer started successfully")
            return [stats_query, alerts_query]
            
        except Exception as e:
            logger.error(f"Failed to start speed layer: {e}")
            # Clean up on failure
            self.cleanup_memory()
            raise
    
    def cleanup_memory(self):
        """Clean up cached DataFrames to free memory"""
        for df in self.cached_dataframes:
            try:
                df.unpersist()
            except Exception as e:
                logger.warning(f"Error unpersisting DataFrame: {e}")
        self.cached_dataframes.clear()
    
    def monitor_memory_usage(self):
        """Monitor Spark memory usage"""
        try:
            sc = self.spark.sparkContext
            status = sc.statusTracker()
            
            for executor in status.getExecutorInfos():
                memory_used = executor.memoryUsed
                max_memory = executor.maxMemory
                usage_percent = (memory_used / max_memory) * 100 if max_memory > 0 else 0
                
                if usage_percent > 80:
                    logger.warning(f"High memory usage on executor {executor.executorId}: {usage_percent:.1f}%")
                    
        except Exception as e:
            logger.error(f"Error monitoring memory: {e}")

if __name__ == "__main__":
    speed_layer = None
    queries = []
    
    try:
        speed_layer = SpeedLayer()
        queries = speed_layer.start_speed_layer()
        
        logger.info("Speed layer running. Press Ctrl+C to stop.")
        
        # Monitor query health and memory
        import time
        while True:
            for i, query in enumerate(queries):
                if not query.isActive:
                    logger.error(f"Query {i} ({query.name}) stopped unexpectedly")
                    if query.exception():
                        logger.error(f"Query exception: {query.exception()}")
            
            # Monitor memory every 5 minutes
            speed_layer.monitor_memory_usage()
            time.sleep(300)
            
    except KeyboardInterrupt:
        logger.info("Shutting down speed layer...")
    except Exception as e:
        logger.error(f"Speed layer failed: {e}")
    finally:
        # Graceful shutdown
        for query in queries:
            if query.isActive:
                logger.info(f"Stopping query: {query.name}")
                query.stop()
        
        if speed_layer:
            # Clean up cached DataFrames
            speed_layer.cleanup_memory()
            speed_layer.spark.stop()
        
        logger.info("Speed layer shutdown complete")