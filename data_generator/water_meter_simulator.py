import json
import pandas as pd
import time
import math
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
                    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                    linger_ms=10  # small buffer to batch messages slightly
                )
            except Exception as e:
                print(f"Kafka not available: {e}")
                self.use_kafka = False
        
        self.data_path = data_path
    
    def clean_json(self, obj):
        if isinstance(obj, dict):
            return {k: self.clean_json(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.clean_json(v) for v in obj]
        elif isinstance(obj, float):
            if math.isnan(obj) or math.isinf(obj):
                return None
            return obj
        else:
            return obj

    def process_csv_file(self, file_path):
        """Memory-efficient row-by-row CSV reading"""
        for chunk in pd.read_csv(file_path, chunksize=1000):  # small chunks
            for _, row in chunk.iterrows():
                reading = {
                    'meter_id': str(row['ManagedObjectid']),
                    'timestamp': str(row['time']),
                    'measurement_type': str(row['typeM']),
                    'series': str(row['Series']) if not pd.isna(row['Series']) else None,
                    'unit': str(row['Unit']) if not pd.isna(row['Unit']) else None,
                    'value': float(row['Value']) if not pd.isna(row['Value']) else None
                }
                try:
                    ts_ms = int(pd.to_datetime(row['time']).timestamp() * 1000)
                except Exception:
                    ts_ms = int(time.time() * 1000)  # fallback to current time if parsing fails

                yield reading, ts_ms
    
    def run(self, delay_seconds=0.01, max_records=None):
        """Stream CSV files efficiently"""
        csv_files = sorted([f for f in os.listdir(self.data_path) if f.endswith('.csv')])
        
        total_records = 0
        for csv_file in csv_files:
            file_path = os.path.join(self.data_path, csv_file)
            print(f"Processing {csv_file}...")
            
            for reading, ts_ms in self.process_csv_file(file_path):
                data = self.clean_json(reading)
                if self.use_kafka and self.producer:
                    self.producer.send('water-meter-readings', value=data, timestamp_ms=ts_ms)
                else:
                    print(json.dumps(reading))
                
                total_records += 1
                if max_records and total_records >= max_records:
                    break
                
                time.sleep(delay_seconds)  # throttle to avoid memory spike
            
            if max_records and total_records >= max_records:
                break
        
        if self.producer:
            self.producer.flush()  # flush remaining messages
            self.producer.close()
        print(f"Data streaming complete. Processed {total_records} records")

if __name__ == "__main__":
    simulator = WaterMeterSimulator()
    print("Starting water meter data generation...")
    simulator.run()