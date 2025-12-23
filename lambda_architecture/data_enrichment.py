import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, when, lit, to_timestamp, current_timestamp, trim, lower, initcap, regexp_extract
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType

logger = logging.getLogger(__name__)

class SparkDataEnrichment:
    def __init__(self, spark: SparkSession, data_path=None):
        """
        Initialize DataEnrichment using Spark DataFrame (broadcast join ready).
        Loads metadata from HDFS if available, otherwise local.
        """
        self.spark = spark
        self.data_path = data_path or "hdfs://namenode:8020/data/water_meter/metadata/managedobject_details.csv"
        self.metadata_df = self._load_meter_details()
        logger.info("DataEnrichment initialized successfully with Spark.")

    def _load_meter_details(self):
        """Load and clean meter metadata into Spark DataFrame and broadcast it."""
        schema = StructType([
            StructField("managedObjects_id", StringType(), True),
            StructField("Meter Type", StringType(), True),
            StructField("Suburb", StringType(), True),
            StructField("Postcode", StringType(), True),
            StructField("Usage Type", StringType(), True),
        ])
        try:
            logger.info(f"📂 Loading metadata from {self.data_path}")
            df = self.spark.read.option("header", "true").schema(schema).csv(self.data_path)
            count = df.count()
            if count == 0:
                logger.warning(f"⚠️ Metadata file is empty: {self.data_path}")
                return self.spark.createDataFrame([], schema)
            
            # Clean metadata like speed layer
            cleaned_df = self._clean_metadata(df)
            logger.info(f"✅ Loaded and cleaned {cleaned_df.count()} meter details from {self.data_path}")
            return broadcast(cleaned_df)

        except Exception as e:
            logger.error(f"❌ Failed to load metadata from {self.data_path}: {e}")
            # Fallback local
            local_path = self._get_local_fallback_path()
            try:
                if os.path.exists(local_path):
                    logger.info(f"📂 Falling back to local metadata: {local_path}")
                    df = self.spark.read.option("header", "true").schema(schema).csv(local_path)
                    cleaned_df = self._clean_metadata(df)
                    return broadcast(cleaned_df)
                else:
                    logger.warning("⚠️ No metadata available (HDFS and local missing). Returning empty DataFrame.")
            except Exception as e2:
                logger.error(f"❌ Failed to load fallback metadata: {e2}")

            # Return empty DataFrame with correct schema
            return broadcast(self.spark.createDataFrame([], schema))
    
    def _clean_metadata(self, meta_df):
        """Clean metadata consistent with speed layer"""
        # Check required columns
        required_cols = ["managedObjects_id", "Meter Type", "Suburb", "Postcode", "Usage Type"]
        missing = [c for c in required_cols if c not in meta_df.columns]
        if missing:
            logger.error(f"Missing required columns: {missing}")
            # Return empty DataFrame with correct schema instead of malformed data
            schema = StructType([
                StructField("managedObjects_id", StringType(), True),
                StructField("Meter Type", StringType(), True),
                StructField("Suburb", StringType(), True),
                StructField("Postcode", StringType(), True),
                StructField("Usage Type", StringType(), True),
            ])
            return self.spark.createDataFrame([], schema)
        
        # Clean and standardize - keep original column names for batch compatibility
        cleaned_df = meta_df.withColumn("managedObjects_id", col("managedObjects_id").cast("string"))
        
        # Clean text fields - trim and lowercase VALUES but keep column names
        for c in ["Suburb", "Usage Type", "Meter Type"]:
            cleaned_df = cleaned_df.withColumn(c, trim(lower(col(c))))
        
        # Clean postcode - extract digits only
        cleaned_df = cleaned_df.withColumn("Postcode", regexp_extract(col("Postcode"), "\\d+", 0))
        
        # Fill nulls with defaults
        cleaned_df = cleaned_df.fillna({
            "Suburb": "unknown", 
            "Postcode": "0000", 
            "Usage Type": "unknown", 
            "Meter Type": "unknown"
        })
        
        # Remove nulls and duplicates
        cleaned_df = cleaned_df.filter(col("managedObjects_id").isNotNull()) \
                              .dropDuplicates(["managedObjects_id"])
        
        # Add display label for suburb
        cleaned_df = cleaned_df.withColumn("Suburb_Label", initcap(col("Suburb")))
        
        return cleaned_df

    def _get_local_fallback_path(self):
        """Get local metadata file path relative to project root."""
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(os.path.dirname(current_dir))
        return os.path.join(project_root, 'data', 'managedobject_details.csv')

    def clean_iot_data(self, raw_df):
        """Clean IoT data - consistent with speed layer logic"""
        logger.info("Starting data cleaning...")
        
        # Remove null records and validate core fields
        cleaned_df = raw_df.dropna(how='all').filter(
            (col("meter_id").isNotNull()) &
            (col("timestamp").isNotNull()) &
            (col("measurement_type").isNotNull()) &
            (col("value").isNotNull())
        )
        
        # Data type conversion and validation
        cleaned_df = cleaned_df.withColumn("meter_id", col("meter_id").cast(StringType())) \
                              .withColumn("value", col("value").cast(DoubleType())) \
                              .withColumn("timestamp_parsed", to_timestamp(col("timestamp")))
        
        # Remove invalid values
        cleaned_df = cleaned_df.filter(
            (col("meter_id").isNotNull()) &
            (col("value") >= 0) &
            (col("value") < 1000000) &
            (col("timestamp_parsed").isNotNull())
        )
        
        # Remove duplicates and standardize measurement types
        cleaned_df = cleaned_df.dropDuplicates(["meter_id", "timestamp", "measurement_type"]) \
                              .withColumn("measurement_type", 
                                  when(col("measurement_type").isin(["pulse1", "PULSE1"]), "Pulse1")
                                  .when(col("measurement_type").isin(["battery", "BATTERY"]), "Battery")
                                  .when(col("measurement_type").isin(["temperature", "TEMPERATURE", "devicetemperature"]), "DeviceTemperature")
                                  .otherwise(col("measurement_type"))
                              )
        
        logger.info("Data cleaning completed.")
        return cleaned_df

    def enrich_dataframe(self, raw_df, skip_cleaning=False):
        """
        Clean and enrich a Spark DataFrame of raw IoT readings with metadata.
        - First cleans the data (unless skip_cleaning=True)
        - Then performs left join on 'meter_id' vs 'managedObjects_id'
        """
        if self.metadata_df.rdd.isEmpty():
            logger.warning("⚠️ Metadata DataFrame is empty. Returning cleaned DataFrame only.")
            return self.clean_iot_data(raw_df) if not skip_cleaning else raw_df

        try:
            # Clean data first (unless skipped)
            working_df = self.clean_iot_data(raw_df) if not skip_cleaning else raw_df
            
            # Then enrich with metadata
            enriched_df = (
                working_df.join(
                    self.metadata_df,
                    working_df["meter_id"] == self.metadata_df["managedObjects_id"],
                    "left"
                )
                .drop("managedObjects_id")  # remove duplicate key
            )
            logger.info("✅ Successfully cleaned and enriched raw data with metadata.")
            return enriched_df
        except Exception as e:
            logger.error(f"❌ Failed to enrich DataFrame: {e}")
            return self.clean_iot_data(raw_df) if not skip_cleaning else raw_df