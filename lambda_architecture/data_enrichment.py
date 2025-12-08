import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

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
        """Load meter metadata into Spark DataFrame and broadcast it."""
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
            else:
                logger.info(f"✅ Loaded {count} meter details from {self.data_path}")
            return broadcast(df)

        except Exception as e:
            logger.error(f"❌ Failed to load metadata from {self.data_path}: {e}")
            # Fallback local
            local_path = self._get_local_fallback_path()
            try:
                if os.path.exists(local_path):
                    logger.info(f"📂 Falling back to local metadata: {local_path}")
                    df = self.spark.read.option("header", "true").schema(schema).csv(local_path)
                    return broadcast(df)
                else:
                    logger.warning("⚠️ No metadata available (HDFS and local missing). Returning empty DataFrame.")
            except Exception as e2:
                logger.error(f"❌ Failed to load fallback metadata: {e2}")

            # Return empty DataFrame with correct schema
            return self.spark.createDataFrame([], schema)

    def _get_local_fallback_path(self):
        """Get local metadata file path relative to project root."""
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(os.path.dirname(current_dir))
        return os.path.join(project_root, 'data', 'managedobject_details.csv')

    def enrich_dataframe(self, raw_df):
        """
        Enrich a Spark DataFrame of raw IoT readings with metadata.
        - Performs left join on 'meter_id' vs 'managedObjects_id'
        """
        if self.metadata_df is None:
            logger.warning("⚠️ Metadata DataFrame is empty. Returning original DataFrame.")
            return raw_df

        try:
            enriched_df = (
                raw_df.join(
                    self.metadata_df,
                    raw_df["meter_id"] == self.metadata_df["managedObjects_id"],
                    "left"
                )
                .drop("managedObjects_id")  # remove duplicate key
            )
            logger.info("✅ Successfully enriched raw data with metadata.")
            return enriched_df
        except Exception as e:
            logger.error(f"❌ Failed to enrich DataFrame: {e}")
            return raw_df