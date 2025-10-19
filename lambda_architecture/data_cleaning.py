from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

logger = logging.getLogger(__name__)

class DataCleaner:
    def __init__(self, spark):
        self.spark = spark
    
    def clean_iot_data(self, raw_df):
        """Comprehensive data cleaning for IoT water meter data"""
        
        logger.info(f"Starting data cleaning. Input records: {raw_df.count()}")
        
        # 1. Remove completely null records
        step1 = raw_df.dropna(how='all')
        
        # 2. Filter valid core fields
        step2 = step1.filter(
            (col("meter_id").isNotNull()) &
            (col("timestamp").isNotNull()) &
            (col("measurement_type").isNotNull()) &
            (col("value").isNotNull())
        )
        
        # 3. Data type validation and conversion
        step3 = step2.withColumn("meter_id", col("meter_id").cast(LongType())) \
                    .withColumn("value", col("value").cast(DoubleType())) \
                    .withColumn("timestamp_parsed", to_timestamp(col("timestamp")))
        
        # 4. Remove invalid values
        step4 = step3.filter(
            (col("meter_id") > 0) &                    # Valid meter IDs
            (col("value") >= 0) &                      # No negative readings
            (col("value") < 1000000) &                 # Remove extreme outliers
            (col("timestamp_parsed").isNotNull())      # Valid timestamps
        )
        
        # 5. Remove duplicate readings (same meter, time, type)
        step5 = step4.dropDuplicates(["meter_id", "timestamp", "measurement_type"])
        
        # 6. Standardize measurement types
        step6 = step5.withColumn("measurement_type", 
            when(col("measurement_type").isin(["pulse1", "PULSE1"]), "Pulse1")
            .when(col("measurement_type").isin(["pulse1_total", "PULSE1_TOTAL"]), "Pulse1_Total")
            .when(col("measurement_type").isin(["battery", "BATTERY"]), "Battery")
            .when(col("measurement_type").isin(["temperature", "TEMPERATURE", "devicetemperature"]), "DeviceTemperature")
            .otherwise(col("measurement_type"))
        )
        
        # 7. Add data quality flags
        cleaned_df = step6.withColumn("is_cleaned", lit(True)) \
                         .withColumn("cleaning_timestamp", current_timestamp())
        
        logger.info(f"Data cleaning completed. Output records: {cleaned_df.count()}")
        
        return cleaned_df
    
    def detect_anomalies(self, df):
        """Detect and flag anomalous readings"""
        
        # Calculate statistical bounds per meter and measurement type
        stats_df = df.groupBy("meter_id", "measurement_type") \
                    .agg(
                        avg("value").alias("avg_value"),
                        stddev("value").alias("stddev_value"),
                        min("value").alias("min_value"),
                        max("value").alias("max_value")
                    )
        
        # Join back with original data
        with_stats = df.join(stats_df, ["meter_id", "measurement_type"])
        
        # Flag outliers (beyond 3 standard deviations)
        anomaly_df = with_stats.withColumn("is_anomaly",
            when(
                (col("value") > col("avg_value") + 3 * col("stddev_value")) |
                (col("value") < col("avg_value") - 3 * col("stddev_value")),
                True
            ).otherwise(False)
        ).drop("avg_value", "stddev_value", "min_value", "max_value")
        
        return anomaly_df
    
    def generate_cleaning_report(self, original_df, cleaned_df):
        """Generate data cleaning summary report"""
        
        original_count = original_df.count()
        cleaned_count = cleaned_df.count()
        removed_count = original_count - cleaned_count
        
        report = {
            "original_records": original_count,
            "cleaned_records": cleaned_count,
            "removed_records": removed_count,
            "removal_percentage": (removed_count / original_count) * 100 if original_count > 0 else 0
        }
        
        logger.info(f"Cleaning Report: {report}")
        return report