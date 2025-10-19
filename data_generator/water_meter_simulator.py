import json
import pandas as pd
import time
import os
from datetime import datetime

class WaterMeterSimulator:
    def __init__(self, kafka_server='localhost:29092', data_path='./data/Digital Meter Data  - July 2022', use_kafka=True):
        self.use_kafka = use_kafka
        self.producer = None
        
        if use_kafka:
            try:
                from kafka import KafkaProducer
                self.producer = KafkaProducer(
                    bootstrap_servers=[kafka_server],
                    value_serializer=lambda x: json.dumps(x).encode('utf-8')
                )
            except Exception as e:
                print(f"Kafka not available: {e}")
                self.use_kafka = False
        
        self.data_path = data_path
    
    def process_csv_file(self, file_path):
        """Process a single CSV file and yield raw IoT readings"""
        df = pd.read_csv(file_path)
        
        for _, row in df.iterrows():
            # IoT meters only send raw sensor data
            reading = {
                'meter_id': row['ManagedObjectid'],
                'timestamp': row['time'],
                'measurement_type': row['typeM'],
                'series': row['Series'],
                'unit': row['Unit'],
                'value': row['Value']
            }
            yield reading
    
    def run(self, delay_seconds=0.1, max_records=None):
        """Stream data from CSV files"""
        csv_files = sorted([f for f in os.listdir(self.data_path) if f.endswith('.csv')])
        
        total_records = 0
        for csv_file in csv_files:
            file_path = os.path.join(self.data_path, csv_file)
            print(f"Processing {csv_file}...")
            
            for reading in self.process_csv_file(file_path):
                if self.use_kafka and self.producer:
                    self.producer.send('water-meter-readings', reading)
                else:
                    print(json.dumps(reading))
                
                total_records += 1
                if max_records and total_records >= max_records:
                    break
                    
                time.sleep(delay_seconds)
            
            if max_records and total_records >= max_records:
                break
        
        if self.producer:
            self.producer.close()
        print(f"Data streaming complete. Processed {total_records} records")

if __name__ == "__main__":
    simulator = WaterMeterSimulator()
    print("Starting water meter data generation...")
    simulator.run()