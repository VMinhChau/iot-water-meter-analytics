from pyspark.sql import SparkSession
import pandas as pd

class TableauHiveConnector:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("TableauHiveConnector") \
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
            .enableHiveSupport() \
            .getOrCreate()
    
    def export_daily_stats(self, output_path='tableau_daily_stats.csv'):
        """Export daily stats from Hive to CSV for Tableau"""
        try:
            df = self.spark.sql("""
                SELECT meter_id, date, measurement_type, 
                       total_value, avg_value, max_value, min_value, reading_count
                FROM water_meter.daily_stats
                WHERE measurement_type IN ('Pulse1', 'Pulse1_Total', 'Battery', 'DeviceTemperature')
            """)
            df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
            print(f"Daily stats exported from Hive to {output_path}")
        except Exception as e:
            print(f"Error exporting daily stats: {e}")
    
    def export_monthly_stats(self, output_path='tableau_monthly_stats.csv'):
        """Export monthly stats from Hive to CSV for Tableau"""
        try:
            df = self.spark.sql("""
                SELECT meter_id, month, measurement_type, 
                       monthly_total, monthly_avg
                FROM water_meter.monthly_stats
                WHERE measurement_type IN ('Pulse1_Total', 'Battery')
            """)
            df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
            print(f"Monthly stats exported from Hive to {output_path}")
        except Exception as e:
            print(f"Error exporting monthly stats: {e}")
    
    def export_problem_meters(self, output_path='tableau_problem_meters.csv'):
        """Export problem meters from Hive to CSV for Tableau"""
        try:
            df = self.spark.sql("""
                SELECT meter_id, date, measurement_type, issue_type,
                       total_value, avg_value, max_value, min_value, reading_count
                FROM water_meter.problem_meters
                ORDER BY date DESC, issue_type
            """)
            df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
            print(f"Problem meters exported from Hive to {output_path}")
        except Exception as e:
            print(f"Error exporting problem meters: {e}")
    
    def get_hive_connection_info(self):
        """Get Hive connection details for direct Tableau connection"""
        return {
            'host': 'localhost',
            'port': 10000,
            'database': 'water_meter',
            'tables': ['daily_stats', 'monthly_stats', 'problem_meters', 'raw_readings']
        }

if __name__ == "__main__":
    connector = TableauHiveConnector()
    connector.export_daily_stats()
    connector.export_monthly_stats()
    connector.export_problem_meters()
    
    # Print Hive connection info for Tableau
    info = connector.get_hive_connection_info()
    print(f"\nTableau Hive Connection:")
    print(f"Host: {info['host']}")
    print(f"Port: {info['port']}")
    print(f"Database: {info['database']}")
    print(f"Tables: {info['tables']}")