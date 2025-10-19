#!/usr/bin/env python3

from water_meter_simulator import WaterMeterSimulator

def demo_real_data_streaming():
    """Demo streaming real water meter data"""
    print("=== Water Meter Real Data Streaming Demo ===")
    
    # Create simulator without Kafka (for demo)
    simulator = WaterMeterSimulator(use_kafka=False)
    
    print("Starting raw IoT data stream (first 10 records)...\n")
    
    # Stream first 10 records with 0.5 second delay
    simulator.run(delay_seconds=0.5, max_records=10)

if __name__ == "__main__":
    demo_real_data_streaming()