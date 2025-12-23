from pyspark.sql import SparkSession
import pandas as pd

class TableauDeltaConnector:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("TableauDeltaConnector") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
    
    def export_daily_stats(self, output_path='tableau_daily_stats.csv'):
        """Export daily stats from Delta Lake to CSV for Tableau"""
        try:
            df = self.spark.read.format("delta").load("hdfs://namenode:8020/data/water_meter/batch/daily_stats")
            df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
            print(f"Daily stats exported from Delta Lake to {output_path}")
        except Exception as e:
            print(f"Error exporting daily stats: {e}")
    
    def export_monthly_stats(self, output_path='tableau_monthly_stats.csv'):
        """Export monthly stats from Delta Lake to CSV for Tableau"""
        try:
            df = self.spark.read.format("delta").load("hdfs://namenode:8020/data/water_meter/batch/monthly_stats")
            df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
            print(f"Monthly stats exported from Delta Lake to {output_path}")
        except Exception as e:
            print(f"Error exporting monthly stats: {e}")
    
    def export_problem_meters(self, output_path='tableau_problem_meters.csv'):
        """Export problem meters from Delta Lake to CSV for Tableau"""
        try:
            df = self.spark.read.format("delta").load("hdfs://namenode:8020/data/water_meter/batch/problem_meters")
            df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
            print(f"Problem meters exported from Delta Lake to {output_path}")
        except Exception as e:
            print(f"Error exporting problem meters: {e}")
    
    def get_spark_thrift_connection_info(self):
        """Get Spark Thrift Server connection details for direct Tableau connection"""
        return {
            'host': 'localhost',
            'port': 10000,
            'database': 'default',
            'tables': ['daily_stats', 'monthly_stats', 'problem_meters']
        }

if __name__ == "__main__":
    connector = TableauDeltaConnector()
    connector.export_daily_stats()
    connector.export_monthly_stats()
    connector.export_problem_meters()
    
    # Print Spark Thrift Server connection info for Tableau
    info = connector.get_spark_thrift_connection_info()
    print(f"\nTableau Spark Thrift Server Connection:")
    print(f"Host: {info['host']}")
    print(f"Port: {info['port']}")
    print(f"Database: {info['database']}")
    print(f"Tables: {info['tables']}")