from databricks import sql
import os
import logging

logger = logging.getLogger(__name__)

class HiveAnalytics:
    def __init__(self):
        self.server_hostname = os.getenv('DATABRICKS_SQL_HOSTNAME')
        self.http_path = os.getenv('DATABRICKS_SQL_HTTP_PATH')
        self.access_token = os.getenv('DATABRICKS_SQL_TOKEN')
    
    def connect(self):
        return sql.connect(
            server_hostname=self.server_hostname,
            http_path=self.http_path,
            access_token=self.access_token
        )
    
    def query_batch_results(self):
        """Query batch processing results from Hive tables"""
        with self.connect() as connection:
            cursor = connection.cursor()
            
            # Show available tables
            cursor.execute("SHOW TABLES IN water_meter")
            tables = cursor.fetchall()
            logger.info(f"Available tables: {tables}")
            
            # Query daily aggregation results
            cursor.execute("""
                SELECT * FROM water_meter.daily_aggregation
                ORDER BY date DESC, avg_flow DESC
                LIMIT 10
            """)
            daily_results = cursor.fetchall()
            
            # Query anomalies
            cursor.execute("""
                SELECT meter_id, suburb, timestamp, flow_rate, z_score
                FROM water_meter.anomalies
                ORDER BY ABS(z_score) DESC
                LIMIT 10
            """)
            anomaly_results = cursor.fetchall()
            
            # Query suburb analysis
            cursor.execute("""
                SELECT suburb, usage_type, meter_count, 
                       ROUND(avg_flow_rate, 2) as avg_flow,
                       ROUND(total_consumption, 2) as total_consumption
                FROM water_meter.suburb_analysis
                ORDER BY total_consumption DESC
            """)
            suburb_results = cursor.fetchall()
            
            return {
                'daily_agg': daily_results,
                'anomalies': anomaly_results,
                'suburb_analysis': suburb_results
            }
    
    def create_business_views(self):
        """Create business intelligence views"""
        with self.connect() as connection:
            cursor = connection.cursor()
            
            # High consumption meters view
            cursor.execute("""
                CREATE OR REPLACE VIEW water_meter.high_consumption_meters AS
                SELECT meter_id, suburb, usage_type, avg_flow, daily_volume
                FROM water_meter.daily_aggregation
                WHERE daily_volume > (
                    SELECT PERCENTILE(daily_volume, 0.9) 
                    FROM water_meter.daily_aggregation
                )
            """)
            
            # Anomaly summary view
            cursor.execute("""
                CREATE OR REPLACE VIEW water_meter.anomaly_summary AS
                SELECT suburb, usage_type,
                       COUNT(*) as anomaly_count,
                       AVG(ABS(z_score)) as avg_anomaly_score
                FROM water_meter.anomalies a
                JOIN water_meter.daily_aggregation d 
                ON a.meter_id = d.meter_id
                GROUP BY suburb, usage_type
                ORDER BY anomaly_count DESC
            """)
            
            logger.info("✓ Business views created")
    
    def generate_reports(self):
        """Generate business reports from Hive tables"""
        with self.connect() as connection:
            cursor = connection.cursor()
            
            # Top consuming suburbs
            cursor.execute("""
                SELECT suburb, 
                       SUM(total_consumption) as total_suburb_consumption,
                       AVG(avg_flow_rate) as avg_suburb_flow,
                       COUNT(DISTINCT meter_count) as total_meters
                FROM water_meter.suburb_analysis
                GROUP BY suburb
                ORDER BY total_suburb_consumption DESC
                LIMIT 5
            """)
            top_suburbs = cursor.fetchall()
            
            # Anomaly trends
            cursor.execute("""
                SELECT DATE(timestamp) as anomaly_date,
                       COUNT(*) as daily_anomalies,
                       AVG(ABS(z_score)) as avg_severity
                FROM water_meter.anomalies
                GROUP BY DATE(timestamp)
                ORDER BY anomaly_date DESC
                LIMIT 7
            """)
            anomaly_trends = cursor.fetchall()
            
            return {
                'top_suburbs': top_suburbs,
                'anomaly_trends': anomaly_trends
            }

if __name__ == "__main__":
    analytics = HiveAnalytics()
    
    # Query batch results
    results = analytics.query_batch_results()
    print("Batch Processing Results from Hive:")
    for table, data in results.items():
        print(f"\n{table.upper()}:")
        for row in data[:3]:  # Show first 3 rows
            print(f"  {row}")
    
    # Create business views
    analytics.create_business_views()
    
    # Generate reports
    reports = analytics.generate_reports()
    print("\nBusiness Reports:")
    for report, data in reports.items():
        print(f"\n{report.upper()}:")
        for row in data:
            print(f"  {row}")