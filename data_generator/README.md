# IoT Water Meter Data Generator

## Overview
Simulates 2,817 real IoT water meters by streaming actual sensor data from July 2022 CSV files.

## How to Run

### Quick Start
```bash
# From project root:
./run_data_generator.sh
```

### Manual Usage
```bash
# Demo (10 records)
python3 demo_real_data.py

# Full streaming
python3 water_meter_simulator.py
```

## Raw IoT Data Structure
Meters send only sensor readings (no metadata):
```json
{
  "meter_id": 83008,
  "timestamp": "2022-07-13T07:30:01.000Z",
  "measurement_type": "Pulse1",
  "series": "P1",
  "unit": "L",
  "value": 6.0
}
```

## Lambda Architecture Integration
```
CSV Files → IoT Simulator → Kafka → ┌─ Speed Layer → Kibana
                                    │
                                    └─ Batch Layer → Tableau
```

### Data Processing
- **Speed Layer**: Real-time stream processing for live monitoring
- **Batch Layer**: Historical analysis for business intelligence
- **Dual output**: Operational dashboards + analytical reports

## Features
- ✅ **Real data** - Actual water meter readings from Queensland
- ✅ **Kafka streaming** - Sends to `water-meter-readings` topic
- ✅ **Multiple types** - Pulse1, Battery, Temperature measurements
- ✅ **High volume** - ~5.2M readings/month simulation
- ✅ **Realistic timing** - 15-minute intervals for flow data

## Data Volume
- **2,817 IoT meters** across Queensland suburbs
- **31 daily CSV files** (~167K readings each)
- **~5.2M readings/month** total volume
- **Multiple measurement types** for comprehensive testing

## Integration with Pipeline
- **Real-time**: Data flows to Kibana for live monitoring
- **Historical**: Batch processing creates Tableau reports
- **Enrichment**: Metadata added during pipeline processing
- **Analytics**: Both operational and business intelligence outputs

**Note**: Raw IoT data is enriched with metadata during Lambda Architecture processing.