from pyspark.sql.types import *

class SchemaRegistry:
    """Centralized schema definitions for IoT water meter data"""
    
    _schemas = {
        "iot_raw": StructType([
            StructField("meter_id", LongType(), False),
            StructField("timestamp", StringType(), False),
            StructField("measurement_type", StringType(), False),
            StructField("value", DoubleType(), False)
        ]),
        
        "iot_enriched": StructType([
            StructField("meter_id", LongType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("measurement_type", StringType(), False),
            StructField("value", DoubleType(), False),
            StructField("meter_type", StringType(), True),
            StructField("suburb", StringType(), True),
            StructField("postcode", IntegerType(), True),
            StructField("usage_type", StringType(), True)
        ]),
        
        "aggregated_stats": StructType([
            StructField("meter_id", LongType(), False),
            StructField("date", DateType(), False),
            StructField("measurement_type", StringType(), False),
            StructField("total_value", DoubleType(), True),
            StructField("avg_value", DoubleType(), True),
            StructField("max_value", DoubleType(), True),
            StructField("min_value", DoubleType(), True),
            StructField("reading_count", LongType(), True)
        ])
    }
    
    @classmethod
    def get_schema(cls, schema_name, version="v1"):
        """Get schema by name"""
        return cls._schemas.get(schema_name)
    
    @classmethod
    def list_schemas(cls):
        """List available schemas"""
        return list(cls._schemas.keys())