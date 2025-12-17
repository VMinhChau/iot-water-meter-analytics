from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, TimestampType, StringType, DoubleType

spark = SparkSession.builder \
    .appName("InitRawDelta") \
    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

path = "hdfs://namenode:8020/data/water_meter/raw_delta"

schema = StructType([
    StructField("meter_id", LongType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("measurement_type", StringType(), True),
    StructField("value", DoubleType(), True)
])

df = spark.createDataFrame([], schema)
df.write.format("delta").mode("overwrite").save(path)

print("✔ Delta table initialized at:", path)
spark.stop()
