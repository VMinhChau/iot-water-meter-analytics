import os
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class Config:
    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS = "iot-water-meter-analytics-kafka-1:9092"
    KAFKA_TOPIC = "water-meter-readings"
    KAFKA_ALERTS_TOPIC = "alerts"
    
    # HDFS Configuration
    HDFS_NAMENODE = "hdfs://namenode:8020"
    HDFS_RAW_PATH = os.getenv("HDFS_RAW_PATH", f"{HDFS_NAMENODE}/data/water_meter/raw")
    HDFS_BATCH_PATH = os.getenv("HDFS_BATCH_PATH", f"{HDFS_NAMENODE}/data/water_meter/batch")
    
    # Elasticsearch Configuration
    ES_HOSTS = "elasticsearch"
    ES_PORT = "9200"
    ES_SPEED_INDEX = "water-meter-speed"
    ES_ALERTS_INDEX = "water-meter-alerts"
    
    # Processing Configuration
    WINDOW_DURATION = "5 minutes"
    SLIDE_DURATION = "1 minute"
    WATERMARK_DELAY = "30 seconds"
    TRIGGER_INTERVAL = "10 seconds"
    
    # Anomaly Thresholds
    HIGH_FLOW_THRESHOLD = float("500")
    LOW_BATTERY_THRESHOLD = float("3.0")
    HIGH_TEMP_THRESHOLD = float("50")
    HIGH_CONSUMPTION_THRESHOLD = float("10000")
    
    # Checkpoint Configuration
    CHECKPOINT_LOCATION = "/tmp/spark_checkpoints"
    
    # Performance Configuration
    SPARK_EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY", "2g")
    SPARK_EXECUTOR_CORES = int(os.getenv("SPARK_EXECUTOR_CORES", "2"))
    SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "1g")
    
    # Data Quality Thresholds
    MAX_NULL_PERCENTAGE = float(os.getenv("MAX_NULL_PERCENTAGE", "5.0"))
    MAX_DUPLICATE_PERCENTAGE = float(os.getenv("MAX_DUPLICATE_PERCENTAGE", "1.0"))
    
    @classmethod
    def validate_config(cls) -> Dict[str, Any]:
        """Validate all configuration values"""
        validation_results = {
            "valid": True,
            "errors": [],
            "warnings": []
        }
        
        # Required configurations
        required_configs = {
            "KAFKA_BOOTSTRAP_SERVERS": cls.KAFKA_BOOTSTRAP_SERVERS,
            "HDFS_RAW_PATH": cls.HDFS_RAW_PATH,
            "ES_HOSTS": cls.ES_HOSTS
        }
        
        for config_name, config_value in required_configs.items():
            if not config_value or config_value == "":
                validation_results["errors"].append(f"Missing required config: {config_name}")
                validation_results["valid"] = False
        
        # Validate thresholds
        if cls.HIGH_FLOW_THRESHOLD <= 0:
            validation_results["warnings"].append("HIGH_FLOW_THRESHOLD should be positive")
        
        if cls.LOW_BATTERY_THRESHOLD <= 0:
            validation_results["warnings"].append("LOW_BATTERY_THRESHOLD should be positive")
        
        # Log results
        if validation_results["errors"]:
            for error in validation_results["errors"]:
                logger.error(error)
        
        if validation_results["warnings"]:
            for warning in validation_results["warnings"]:
                logger.warning(warning)
        
        if validation_results["valid"]:
            logger.info("Configuration validation passed")
        
        return validation_results
    
    @classmethod
    def get_spark_config(cls) -> Dict[str, str]:
        """Get optimized Spark configuration"""
        return {
            "spark.executor.memory": cls.SPARK_EXECUTOR_MEMORY,
            "spark.executor.cores": str(cls.SPARK_EXECUTOR_CORES),
            "spark.driver.memory": cls.SPARK_DRIVER_MEMORY,
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
            "spark.sql.adaptive.coalescePartitions.minPartitionNum": "1",
            "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256MB",
            "spark.sql.files.maxPartitionBytes": "128MB",
            "spark.sql.shuffle.partitions": "200",
            "spark.default.parallelism": "100"
        }