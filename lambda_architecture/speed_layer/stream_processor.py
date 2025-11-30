import logging
import time
import uuid

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, sum, count, avg, max, min, current_timestamp, concat,
    date_format, from_json, trim, lower, initcap, regexp_extract,
    when, window, expr, approx_count_distinct, to_timestamp, countDistinct, broadcast
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.window import Window
from pyspark import StorageLevel
from config import Config

from pyspark.sql.functions import struct, col, lit, date_format, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SpeedLayer")


class SpeedLayer:
    def __init__(self, config: Config = None):
        self.config = config or Config()

        self.spark = (
            SparkSession.builder
            .appName("WaterMeterSpeedLayer")
            .config("spark.es.nodes", self.config.ES_HOSTS)
            .config("spark.es.port", str(self.config.ES_PORT))
            .config("spark.master", "spark://spark-master:7077")
            .config("spark.es.nodes.wan.only", "true")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.shuffle.partitions", "100")
            .getOrCreate()
        )

        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("SparkSession created for SpeedLayer")

        # Load metadata ONCE
        self.metadata = broadcast(self.load_metadata())
        logger.info(f"Loaded metadata: {self.metadata.count()} entries")

    # ================================================================
    # Schema for Kafka messages
    # ================================================================
    def get_schema(self):
        return StructType([
            StructField("meter_id", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("measurement_type", StringType(), True),
            StructField("series", StringType(), True),
            StructField("unit", StringType(), True),
            StructField("value", DoubleType(), True),
        ])

    # ================================================================
    # Kafka stream
    # ================================================================
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

    # ================================================================
    # Metadata loader — loaded ONCE
    # ================================================================
    def load_metadata(self):
        meta_df = self.spark.read.csv("/tmp/managedobject_details.csv", header=True)
        required_cols = ["managedObjects_id", "Meter Type", "Suburb", "Postcode", "Usage Type"]
        missing = [c for c in required_cols if c not in meta_df.columns]
        if missing:
            raise ValueError(f"[Metadata Error] Missing required columns: {missing}")

        meta_df = (
            meta_df
            .withColumnRenamed("managedObjects_id", "meter_id")
            .withColumnRenamed("Meter Type", "meter_type")
            .withColumnRenamed("Suburb", "suburb")
            .withColumnRenamed("Postcode", "postcode")
            .withColumnRenamed("Usage Type", "usage_type")
        )
        meta_df = meta_df.withColumn("meter_id", col("meter_id").cast("string"))
        for c in ["suburb", "usage_type", "meter_type"]:
            meta_df = meta_df.withColumn(c, trim(lower(col(c))))
        meta_df = meta_df.withColumn("postcode", regexp_extract(col("postcode"), "\\d+", 0))
        meta_df = meta_df.fillna({"suburb": "unknown", "postcode": "0000", "usage_type": "unknown", "meter_type": "unknown"})
        meta_df = meta_df.filter(col("meter_id").isNotNull()).dropDuplicates(["meter_id"])
        meta_df = meta_df.withColumn("suburb_label", initcap(col("suburb")))

        return meta_df

    # ================================================================
    # Clean IoT data
    # ================================================================
    def clean_iot_data(self, df):
        df = df.withColumn("timestamp_parsed", to_timestamp(col("timestamp"))) \
               .withColumn("meter_id", col("meter_id").cast("string")) \
               .withColumn("value", col("value").cast("double"))
        for c in ["measurement_type", "unit"]:
            if c in df.columns:
                df = df.withColumn(c, trim(lower(col(c))))
        df = df.dropna(subset=["meter_id", "timestamp_parsed", "value", "measurement_type"])
        df = df.filter((col("value") >= 0) & (col("value") <= 1e9))
        df = df.dropDuplicates(["meter_id", "timestamp_parsed", "measurement_type"])
        # df = df.join(self.metadata, on="meter_id", how="left")

        return df

    # ================================================================
    # Split measurements
    # ================================================================
    def split_measurements(self, df):
        df = df.withColumn("measurement_type_clean", trim(lower(col("measurement_type"))))
        water_types = ["pulse1", "pulse1_total", "10266/0", "10266/1", "10267/0"]
        device_types = ["battery", "devicetemperature", "switch2"]
        return df.filter(col("measurement_type_clean").isin(water_types)), \
               df.filter(col("measurement_type_clean").isin(device_types))

    # ================================================================
    # MAIN STREAMING PROCESS
    # ================================================================
    # Modular helper: compute water statistics
    # ================================================================
    def build_water_stats(self, water_df):
        meta_cols = ["meter_id", "suburb", "meter_type", "usage_type", "postcode", "suburb_label"]
        metadata_sel = self.metadata.select(*meta_cols).select(
            col("meter_id"),
            col("suburb").alias("meta_suburb"),
            col("meter_type").alias("meta_meter_type"),
            col("usage_type").alias("meta_usage_type"),
            col("postcode").alias("meta_postcode"),
            col("suburb_label").alias("meta_suburb_label")
        )

        water_df = water_df.join(metadata_sel, on="meter_id", how="left")

        water_stats_df = (
            water_df
            .filter(col("measurement_type_clean").isin("pulse1", "pulse1_total"))
            .withWatermark("timestamp_parsed", "10 minutes")
        )

        stats = (
            water_stats_df
            .groupBy(
                "meter_id",
                "measurement_type_clean",
                "meta_suburb", "meta_meter_type", "meta_usage_type",
                window(col("timestamp_parsed"), self.config.WINDOW_DURATION, self.config.SLIDE_DURATION)
            )
            .agg(
                sum("value").alias("total_value"),
                count("value").alias("reading_count"),
                avg("value").alias("avg_value"),
                max("value").alias("max_value"),
                min("value").alias("min_value"),
                min("timestamp_parsed").alias("timestamp_min"),
                max("timestamp_parsed").alias("timestamp_max"),
                current_timestamp().alias("processed_time")
            )
            .withColumn(
                "doc_id",
                concat(
                    col("meter_id"), lit("_"),
                    col("measurement_type_clean"), lit("_"),
                    date_format(col("timestamp_max"), "yyyyMMddHHmmss")
                )
            )
            .drop("window")
        )
        return stats

    # ================================================================
    # Modular helper: build anomalies
    # ================================================================
    def build_anomalies(self, water_df, device_df):
        HIGH_FLOW = self.config.HIGH_FLOW_THRESHOLD
        ZERO_THRESHOLD = 0
        IMPOSSIBLE_MAX = 1e6

        pulse1_anomalies = (
            water_df.filter(col("measurement_type_clean") == "pulse1")
            .withColumn("alert_type", when(col("value") > HIGH_FLOW, "HIGH_FLOW"))
            .withColumn("severity", when(col("alert_type") == "HIGH_FLOW", "WARNING"))
            .withColumn("alert_timestamp", current_timestamp())
            .withColumn("doc_id", concat(col("meter_id"), lit("_"),
                                        date_format(col("timestamp_parsed"), "yyyyMMddHHmmss"),
                                        lit("_"), expr("uuid()")))
            .filter(col("alert_type").isNotNull())
        )

        neg_impossible = (
            water_df.filter(col("measurement_type_clean").isin("10266/0", "10266/1", "10267/0"))
            .withColumn(
                "alert_type",
                when(col("value") < 0, "NEGATIVE_VALUE")
                .when(col("value") > IMPOSSIBLE_MAX, "IMPOSSIBLE_VALUE")
            )
            .withColumn("severity", lit("CRITICAL"))
            .withColumn("alert_timestamp", current_timestamp())
            .withColumn("doc_id", concat(col("meter_id"), lit("_"),
                                        date_format(col("timestamp_parsed"), "yyyyMMddHHmmss"),
                                        lit("_"), expr("uuid()")))
            .filter(col("alert_type").isNotNull())
        )

        zero_reading = (
            water_df.filter(col("value") == ZERO_THRESHOLD)
            .withColumn("alert_type", lit("ZERO_READING"))
            .withColumn("severity", lit("WARNING"))
            .withColumn("alert_timestamp", current_timestamp())
            .withColumn("doc_id", concat(col("meter_id"), lit("_"),
                                        date_format(col("timestamp_parsed"), "yyyyMMddHHmmss"),
                                        lit("_"), expr("uuid()")))
        )

        device_anomalies = (
            device_df.filter(
                ((col("measurement_type_clean") == "battery") & (col("value") < self.config.LOW_BATTERY_THRESHOLD)) |
                ((col("measurement_type_clean") == "devicetemperature") & (col("value") > self.config.HIGH_TEMP_THRESHOLD))
            )
            .withColumn("alert_type", when(col("measurement_type_clean") == "battery", "LOW_BATTERY").otherwise("HIGH_TEMPERATURE"))
            .withColumn("severity", when(col("alert_type") == "LOW_BATTERY", "CRITICAL").otherwise("WARNING"))
            .withColumn("alert_timestamp", current_timestamp())
            .withColumn("doc_id", concat(col("meter_id"), lit("_"),
                                        date_format(col("timestamp_parsed"), "yyyyMMddHHmmss"),
                                        lit("_"), expr("uuid()")))
        )

        anomaly_cols = ["meter_id", "timestamp_parsed", "measurement_type_clean", "value", "alert_type",
                        "severity", "alert_timestamp", "doc_id"]

        def align(df):
            for c in anomaly_cols:
                if c not in df.columns:
                    df = df.withColumn(c, lit(None))
            return df.select(*anomaly_cols)

        anomalies = align(pulse1_anomalies).unionByName(align(neg_impossible)).unionByName(align(zero_reading)) \
                                           .unionByName(align(device_anomalies))
        return anomalies

    # ================================================================
    # Modular helper: leak detection
    # ================================================================
    def build_leak_detection(self, water_stats_df):
        # Flag as leak if average flow exceeds threshold and reading count is above minimum
        leak_df = (
            water_stats_df
            # Add a leak_flag column: 1 if both avg_value > threshold and reading_count > 5, else 0
            .withColumn(
                "leak_flag",
                when(
                    (col("avg_value") > self.config.HIGH_FLOW_THRESHOLD) &
                    (col("reading_count") > 5),
                    lit(1)
                ).otherwise(0)
            )
            # Set alert type for records flagged as leak
            .withColumn("alert_type", when(col("leak_flag") == 1, "LEAK_SUSPECT"))
            # Set severity level for leak alerts
            .withColumn("severity", when(col("leak_flag") == 1, "CRITICAL"))
            # Add current timestamp as alert creation time
            .withColumn("alert_timestamp", current_timestamp())
            # Generate a unique document ID for ES for each leak alert
            .withColumn(
                "doc_id",
                concat(
                    col("meter_id"), lit("_LEAK_"),
                    date_format(col("timestamp_max"), "yyyyMMddHHmmss"),
                    lit("_"), expr("uuid()")
                )
            )
            # Only keep rows that are flagged as leaks
            .filter(col("leak_flag") == 1)
        )
        return leak_df

    # ================================================================
        
    def process_realtime_stream(self):
        raw_stream = self.create_kafka_stream()
        schema = self.get_schema()

        parsed = raw_stream.select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ).select("data.*", "kafka_timestamp")

        cleaned = self.clean_iot_data(parsed)
        water_df, device_df = self.split_measurements(cleaned)

        # --- Modular: compute stats ---
        stats = self.build_water_stats(water_df)

        # --- Modular: compute anomalies ---
        anomalies = self.build_anomalies(water_df, device_df)

        # --- Modular: leak detection ---
        leak_df = self.build_leak_detection(stats)

        # --- Modular: trend detection ---
        # trend_df = self.build_trend(stats)

        # Merge leak detection into anomalies
        anomalies = anomalies.unionByName(leak_df, allowMissingColumns=True)

        # --- Active distribution ---
        active_distribution = self.metadata.groupBy(
            "suburb", "meter_type", "usage_type"
        ).agg(
            countDistinct("meter_id").alias("total_count")
        ).withColumn("active_count", lit(None).cast(IntegerType())) \
        .withColumn("silent_count", lit(None).cast(IntegerType())) \
        .withColumn("pct_active", lit(None).cast(DoubleType())) \
        .withColumn("pct_silent", lit(None).cast(DoubleType()))

        # Join stats with active_distribution
        stats_df = stats.join(
            active_distribution,
            (stats.meta_suburb == active_distribution.suburb) &
            (stats.meta_meter_type == active_distribution.meter_type) &
            (stats.meta_usage_type == active_distribution.usage_type),
            how="left"
        ).fillna({
            "active_count": 0,
            "silent_count": 0,
            "pct_active": 0.0,
            "pct_silent": 0.0
        })

        # Drop original active_distribution columns to avoid duplicate fields
        stats_df = stats_df.drop("suburb", "meter_type", "usage_type")

        # --- Pulse1_total stream ---
        pulse1_total_df = water_df.filter(col("measurement_type_clean") == "pulse1_total")

        return stats_df, anomalies, pulse1_total_df

    # ================================================================
    # WRITE BATCH TO ES
    # ================================================================
    def _write_batch_to_es(self, df, epoch_id, index_name, upsert_id_col=None):
        if df is None or df.rdd.isEmpty():
            logger.info(f"Empty batch for {index_name}")
            return
        enriched = df.withColumn("batch_id", lit(str(epoch_id))).withColumn("ingestion_time", current_timestamp())
        writer = (enriched.write
                .format("org.elasticsearch.spark.sql")
                .option("es.nodes", self.config.ES_HOSTS)
                .option("es.port", str(self.config.ES_PORT))
                .option("es.nodes.wan.only", "true")
                .mode("append"))
        if upsert_id_col:
            writer = writer.option("es.mapping.id", upsert_id_col)
        writer.save(index_name)
        logger.info(f"Written batch to ES index={index_name}")

    # ================================================================
    # RUN STREAMING
    # ================================================================
    def start_speed_layer(self):
        stats_df, anomalies_df, _ = self.process_realtime_stream()
        stats_query = stats_df.writeStream.foreachBatch(
            lambda df, epoch: self._write_batch_to_es(df, epoch, self.config.ES_SPEED_INDEX, upsert_id_col="doc_id")
        ).outputMode("append") \
        .option("checkpointLocation", f"{self.config.CHECKPOINT_LOCATION}/stats") \
        .queryName("speed_stats") \
        .trigger(processingTime=self.config.TRIGGER_INTERVAL).start()
        anomalies_query = anomalies_df.writeStream.foreachBatch(
            lambda df, epoch: self._write_batch_to_es(df, epoch, self.config.ES_ALERTS_INDEX)
        ).outputMode("append") \
        .option("checkpointLocation", f"{self.config.CHECKPOINT_LOCATION}/anomalies") \
        .queryName("speed_anomalies") \
        .trigger(processingTime=self.config.TRIGGER_INTERVAL).start()
        logger.info("Speed layer running.")
        return [stats_query, anomalies_query]

    def stop_queries(self, queries):
        for q in queries:
            try:
                if q.isActive:
                    q.stop()
            except:
                pass

    def monitor_queries(self, queries, interval=60):
        for q in queries:
            logger.info(f"{q.name} active={q.isActive} lastProgress={q.lastProgress}")


if __name__ == "__main__":
    conf = Config()
    layer = SpeedLayer(conf)
    qs = layer.start_speed_layer()
    try:
        while True:
            layer.monitor_queries(qs)
            time.sleep(60)
    except KeyboardInterrupt:
        pass
    finally:
        layer.stop_queries(qs)
        layer.spark.stop()