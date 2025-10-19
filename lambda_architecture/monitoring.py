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