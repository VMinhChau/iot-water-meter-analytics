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
            .config("spark.es.port", str(self.config.ES_PORT))
            .config("spark.master", "spark://spark-master:7077")
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

    def load_metadata(self):
        """
        Load and clean metadata for IoT devices.
        Metadata ít thay đổi → có thể xử lý sạch kỹ.
        Returns a clean Spark DataFrame.
        """

        # 1. Load from mounted path (nhanh & không cần docker cp)
        meta_df = self.spark.read.csv("/tmp/managedobject_details.csv", header=True)

        # 2. Validate schema quan trọng
        required_cols = ["managedObjects_id", "Meter Type", "Suburb", "Postcode", "Usage Type"]
        missing = [c for c in required_cols if c not in meta_df.columns]
        if missing:
            raise ValueError(f"[Metadata Error] Missing required columns: {missing}")

        # 3. Chuẩn hoá schema
        meta_df = meta_df.withColumnRenamed("managedObjects_id", "meter_id")
        meta_df = meta_df.withColumn("meter_id", col("meter_id").cast("string"))

        # 4. Chuẩn hoá categorical fields
        categorical_cols = ["Suburb", "Usage Type", "Meter Type"]
        for c in categorical_cols:
            meta_df = meta_df.withColumn(c, trim(lower(col(c))))

        # 5. Chuẩn hoá Postcode (giữ lại số)
        meta_df = meta_df.withColumn("Postcode", regexp_extract(col("Postcode"), "\\d+", 0))

        # 6. Fill default (tránh null trong ES + dashboard)
        meta_df = meta_df.fillna({
            "Suburb": "unknown",
            "Postcode": "0000",
            "Usage Type": "unknown",
            "Meter Type": "unknown"
        })

        # 7. Filter meter_id invalid
        meta_df = meta_df.filter(col("meter_id").isNotNull())

        # 8. Deduplicate
        meta_df = meta_df.dropDuplicates(["meter_id"])

        # 9. Add readable label for Dashboard (suburb)
        meta_df = meta_df.withColumn("Suburb_label", initcap(col("Suburb")))

        return meta_df


    def clean_iot_data(self, df):
        # 1. Parse & cast types
        df = df.withColumn("timestamp_parsed", to_timestamp(col("timestamp")))
        df = df.withColumn("meter_id", col("meter_id").cast("string"))
        df = df.withColumn("value", col("value").cast("double"))

        # 2. Standardize categorical
        for c in ["measurement_type", "unit"]:
            df = df.withColumn(c, trim(lower(col(c))))

        # 3. Drop missing key fields
        df = df.dropna(subset=["meter_id", "timestamp_parsed", "value", "measurement_type"])

        # 4. Filter invalid values / outliers
        df = df.filter((col("value") >= 0) & (col("value") <= 1e9))

        # 5. Noise filtering / dedup
        w = Window.partitionBy("meter_id").orderBy("timestamp_parsed")
        df = df.withColumn("prev_value", lag("value").over(w))
        df = df.filter((col("prev_value").isNull()) | ((col("value") - col("prev_value")) <= 1000))
        
        # 6. Deduplication
        df = df.dropDuplicates(["meter_id", "timestamp_parsed", "measurement_type"])

        # 7. Join metadata
        meta_df = self.load_metadata()  # chuẩn hóa, fillna trong hàm này
        df = df.join(broadcast(meta_df), on="meter_id", how="left")

        return df

    def split_measurements(df):
        # chuẩn hóa measurement_type trước
        df = df.withColumn("measurement_type_clean", trim(lower(col("measurement_type"))))

        water_types = ["pulse1", "pulse1_total"]
        device_types = ["battery", "devicetemperature", "switch2"]

        water_df = df.filter(col("measurement_type_clean").isin(water_types))
        device_df = df.filter(col("measurement_type_clean").isin(device_types))

        return water_df, device_df

    def process_realtime_stream(self):
        raw_stream = self.create_kafka_stream()
        schema = self.get_schema()

        # Parse Kafka JSON
        parsed = raw_stream.select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ).select("data.*", "kafka_timestamp")

        parsed = parsed.withColumn("timestamp_parsed", to_timestamp(col("timestamp")))

        # Clean + metadata join
        cleaned = self.clean_iot_data(parsed)
        water_df, device_df = self.split_measurements(cleaned)

        # ================================================================
        # 1️⃣ Stats (pulse1 & pulse1_total)
        # ================================================================
        stats = (
            water_df.filter(col("measurement_type_clean").isin("pulse1", "pulse1_total"))
            .withWatermark("timestamp_parsed", self.config.WATERMARK_DELAY)
            .groupBy(
                window(col("timestamp_parsed"), self.config.WINDOW_DURATION, self.config.SLIDE_DURATION),
                col("meter_id"),
                col("measurement_type_clean")
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

        stats = stats.withColumn("window_start",
                                date_format(col("window").start, "yyyy-MM-dd'T'HH:mm:ss"))

        stats = stats.withColumn(
            "doc_id",
            concat(col("meter_id"), lit("_"),
                col("measurement_type_clean"), lit("_"), col("window_start"))
        )

        # ================================================================
        # 2️⃣ Water anomalies (KHÔNG dùng delta)
        # ================================================================
        water_thresholds = {
            "pulse1": {"high_flow": self.config.HIGH_FLOW_THRESHOLD},
            "pulse1_total": {"max_delta": self.config.PULSE1_TOTAL_MAX_DELTA},  # xử lý riêng
        }

        # pulse1 → high flow
        pulse1_anomalies = (
            water_df.filter(col("measurement_type_clean") == "pulse1")
            .withColumn(
                "alert_type",
                when(col("value") > water_thresholds["pulse1"]["high_flow"], "HIGH_FLOW")
            )
            .withColumn("severity", when(col("alert_type") == "HIGH_FLOW", "WARNING"))
            .withColumn("alert_timestamp", current_timestamp())
            .withColumn(
                "doc_id",
                concat(col("meter_id"), lit("_"),
                    date_format(col("timestamp_parsed"), "yyyyMMddHHmmss"),
                    lit("_"), expr("uuid()"))
            )
            .filter(col("alert_type").isNotNull())
        )

        # fixed-range anomalies
        other_water_anomalies = (
            water_df.filter(col("measurement_type_clean").isin("10266/0", "10266/1", "10267/0"))
            .withColumn(
                "alert_type",
                when((col("value") < 0) | (col("value") > 1e6), "VALUE_OUT_OF_RANGE")
            )
            .withColumn("severity", lit("CRITICAL"))
            .withColumn("alert_timestamp", current_timestamp())
            .withColumn(
                "doc_id",
                concat(col("meter_id"), lit("_"),
                    date_format(col("timestamp_parsed"), "yyyyMMddHHmmss"),
                    lit("_"), expr("uuid()"))
            )
            .filter(col("alert_type").isNotNull())
        )

        # IMPORTANT: pulse1_total không tạo delta trong streaming
        # chỉ lọc ra, không thêm prev_value/delta
        pulse1_total_df = water_df.filter(col("measurement_type_clean") == "pulse1_total")

        # Merge các loại anomalies không cần delta
        water_anomalies = pulse1_anomalies.unionByName(other_water_anomalies)

        # ================================================================
        # 3️⃣ Device anomalies
        # ================================================================
        device_anomalies = (
            device_df.filter(
                ((col("measurement_type_clean") == "battery") &
                (col("value") < self.config.LOW_BATTERY_THRESHOLD)) |
                ((col("measurement_type_clean") == "devicetemperature") &
                (col("value") > self.config.HIGH_TEMP_THRESHOLD))
            )
            .withColumn(
                "alert_type",
                when(col("measurement_type_clean") == "battery", "LOW_BATTERY")
                .otherwise("HIGH_TEMPERATURE")
            )
            .withColumn(
                "severity",
                when(col("alert_type") == "LOW_BATTERY", "CRITICAL")
                .otherwise("WARNING")
            )
            .withColumn("alert_timestamp", current_timestamp())
            .withColumn(
                "doc_id",
                concat(col("meter_id"), lit("_"),
                    date_format(col("timestamp_parsed"), "yyyyMMddHHmmss"),
                    lit("_"), expr("uuid()"))
            )
        )

        anomalies = water_anomalies.unionByName(device_anomalies)

        # ================================================================
        # 4️⃣ RETURN — pulse1_total anomalies sẽ xử lý delta ngoài streaming
        # ================================================================
        return stats, anomalies, pulse1_total_df


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
            lambda df, epoch_id: self._write_batch_to_es(df, epoch_id, self.config.ES_SPEED_INDEX, upsert_id_col="doc_id")
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
        df = df.join(self.metadata, on="meter_id", how="left")

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
    def process_realtime_stream(self):
        raw_stream = self.create_kafka_stream()
        schema = self.get_schema()
        parsed = raw_stream.select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ).select("data.*", "kafka_timestamp")

        cleaned = self.clean_iot_data(parsed)
        water_df, device_df = self.split_measurements(cleaned)

        # --- Join metadata ---
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

        # --- Water stats aggregation ---
        water_stats_df = water_df.filter(col("measurement_type_clean").isin("pulse1", "pulse1_total")) \
                                 .withWatermark("timestamp_parsed", "10 minutes")

        stats = water_stats_df.groupBy(
            "meter_id",
            "measurement_type_clean",
            "meta_suburb", "meta_meter_type", "meta_usage_type",
            window(col("timestamp_parsed"), self.config.WINDOW_DURATION, self.config.SLIDE_DURATION)
        ).agg(
            sum("value").alias("total_value"),
            count("value").alias("reading_count"),
            avg("value").alias("avg_value"),
            max("value").alias("max_value"),
            min("value").alias("min_value"),
            current_timestamp().alias("processed_time")
        ).withColumn("window_start", date_format(col("window").start, "yyyy-MM-dd'T'HH:mm:ss")) \
         .withColumn("doc_id", concat(col("meter_id"), lit("_"), col("measurement_type_clean"), lit("_"), col("window_start"))) \
         .drop("window")

        # --- Active distribution ---
        active_distribution = self.metadata.groupBy(
            "suburb", "meter_type", "usage_type"
        ).agg(
            countDistinct("meter_id").alias("total_count")
        ).withColumnRenamed("suburb", "meta_suburb") \
         .withColumnRenamed("meter_type", "meta_meter_type") \
         .withColumnRenamed("usage_type", "meta_usage_type") \
         .withColumn("active_count", lit(None).cast(IntegerType())) \
         .withColumn("silent_count", lit(None).cast(IntegerType())) \
         .withColumn("pct_active", lit(None).cast(DoubleType())) \
         .withColumn("pct_silent", lit(None).cast(DoubleType()))

        stats_df = stats.join(
            active_distribution,
            (stats.meta_suburb == active_distribution.meta_suburb) &
            (stats.meta_meter_type == active_distribution.meta_meter_type) &
            (stats.meta_usage_type == active_distribution.meta_usage_type),
            how="left"
        ).fillna({
            "active_count": 0,
            "silent_count": 0,
            "pct_active": 0.0,
            "pct_silent": 0.0
        })

        # --- Water anomalies ---
        HIGH_FLOW = self.config.HIGH_FLOW_THRESHOLD
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

        fixed_range = (
            water_df.filter(col("measurement_type_clean").isin("10266/0", "10266/1", "10267/0"))
            .withColumn("alert_type", when((col("value") < 0) | (col("value") > 1e6), "VALUE_OUT_OF_RANGE"))
            .withColumn("severity", lit("CRITICAL"))
            .withColumn("alert_timestamp", current_timestamp())
            .withColumn("doc_id", concat(col("meter_id"), lit("_"),
                                         date_format(col("timestamp_parsed"), "yyyyMMddHHmmss"),
                                         lit("_"), expr("uuid()")))
            .filter(col("alert_type").isNotNull())
        )

        # --- Device anomalies ---
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

        # --- Align anomaly schema ---
        anomaly_cols = ["meter_id", "timestamp_parsed", "measurement_type_clean", "value", "alert_type",
                        "severity", "alert_timestamp", "doc_id", "meta_meter_type", "meta_suburb", "meta_postcode",
                        "meta_usage_type", "meta_suburb_label"]

        def align(df):
            for c in anomaly_cols:
                if c not in df.columns:
                    df = df.withColumn(c, lit(None))
            return df.select(*anomaly_cols)

        water_anomalies = align(pulse1_anomalies).unionByName(align(fixed_range))
        anomalies = water_anomalies.unionByName(align(device_anomalies))

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