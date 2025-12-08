import json
import pandas as pd
import time
import os
from math import ceil

class WaterMeterSimulator:
    def __init__(self, kafka_server='localhost:29092', 
                 data_path='./data/Digital Meter Data  - July 2022', 
                 use_kafka=True, topic='water-meter-readings'):
        self.use_kafka = use_kafka
        self.producer = None
        self.topic = topic
        
        if use_kafka:
            try:
                from kafka import KafkaProducer
                self.producer = KafkaProducer(
                    bootstrap_servers=[kafka_server],
                    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                    linger_ms=50,        # allow small batching
                    batch_size=16384     # 16 KB batch size
                )
                print("✅ Kafka producer initialized")
            except Exception as e:
                print(f"⚠️ Kafka not available: {e}")
                self.use_kafka = False

        self.data_path = data_path

    def process_csv_file(self, file_path, chunksize=1000):
        """Memory-efficient generator row-by-row"""
        for chunk in pd.read_csv(file_path, chunksize=chunksize):
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
                    ts_ms = int(time.time() * 1000)
                yield reading, ts_ms

    def run(self, target_duration_sec=120, batch_size=500):
        """Stream all CSV files, each file over target_duration_sec"""
        csv_files = sorted([f for f in os.listdir(self.data_path) if f.endswith('.csv')])
        if not csv_files:
            print("❌ No CSV files found")
            return

        for csv_file in csv_files:
            file_path = os.path.join(self.data_path, csv_file)
            print(f"\n📂 Processing {csv_file} (~{target_duration_sec}s)")

            # Tính tổng số record
            total_records = sum(len(chunk) for chunk in pd.read_csv(file_path, chunksize=1000))
            if total_records == 0:
                print("⚠️ CSV file empty, skip")
                continue

            delay_per_record = target_duration_sec / total_records
            print(f"Total records: {total_records}, Batch size: {batch_size}, "
                  f"Delay per record: {delay_per_record:.4f}s")

            sent_records = 0
            batch = []

            for reading, ts_ms in self.process_csv_file(file_path):
                batch.append((reading, ts_ms))
                if len(batch) >= batch_size:
                    for r, ts in batch:
                        if self.use_kafka and self.producer:
                            self.producer.send(self.topic, value=r, timestamp_ms=ts)
                    if self.use_kafka and self.producer:
                        self.producer.flush()
                    # Sleep tổng cho toàn batch
                    time.sleep(delay_per_record * len(batch))
                    sent_records += len(batch)
                    batch.clear()
                    progress = sent_records / total_records * 100
                    print(f"📤 Progress: {progress:.2f}%")

            # Gửi batch còn lại
            if batch:
                for r, ts in batch:
                    if self.use_kafka and self.producer:
                        self.producer.send(self.topic, value=r, timestamp_ms=ts)
                if self.use_kafka and self.producer:
                    self.producer.flush()
                sent_records += len(batch)

            print(f"✅ Finished {csv_file}. Sent {sent_records} records over {target_duration_sec}s.")

        if self.producer:
            self.producer.close()

        print("\n🚀 All CSV files processed successfully!")

if __name__ == "__main__":
    simulator = WaterMeterSimulator()
    print("🚀 Starting multi-file water meter data generation demo...")
    simulator.run(target_duration_sec=120, batch_size=500)
