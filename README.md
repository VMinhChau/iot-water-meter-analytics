# IoT Water Meter Analytics - Lambda Architecture

## Overview
Real-time analytics system processing actual IoT water meter data using Lambda Architecture with Kafka, Spark, HDFS, and Elasticsearch.

## Lambda Architecture Data Flow
```
IoT Meters → Kafka → ┌─ Speed Layer → Elasticsearch → Kibana (Real-time)
                     │
                     └─ Batch Layer → HDFS → Hive → Tableau (Historical)
```

### Speed Layer (Real-time Processing)
- ⚡ **Spark Streaming** - 5-minute sliding windows
- 🔍 **Elasticsearch** - Real-time indexing
- 📊 **Kibana** - Live dashboards and monitoring
- 🚨 **Instant alerts** - Anomaly detection (high flow, low battery)

### Batch Layer (Historical Processing)
- 🕐 **Hourly jobs** - Deep analysis of accumulated data
- 💾 **HDFS storage** - Raw data preservation with partitioning
- 🗄️ **Hive tables** - Data warehouse for complex queries
- 📈 **Tableau** - Historical reports and business analytics

## Data Schema

### Raw IoT Data (from meters):
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

### Enriched Data (stored in HDFS):
```json
{
  "meter_id": 83008,
  "timestamp": "2022-07-13T07:30:01.000Z",
  "measurement_type": "Pulse1",
  "series": "P1",
  "unit": "L",
  "value": 6.0,
  "meter_type": "captis_pulse",
  "suburb": "BUDERIM",
  "postcode": 4556,
  "usage_type": "Residential"
}
```

## Implementation Components

### Data Ingestion
- **Raw Data**: IoT meters send sensor readings to Kafka
- **Enrichment**: Data ingestion service adds meter metadata
- **Storage**: Enriched data stored in HDFS as JSON

### Batch Layer
- **Processing**: Spark reads JSON from HDFS
- **Aggregations**: Daily/monthly stats by measurement_type
- **Data Warehouse**: Hive tables for historical analysis
- **Anomaly Detection**: Identifies problem meters (high consumption, low battery)

### Speed Layer
- **Real-time Processing**: Spark Streaming processes Kafka data
- **Windowing**: 5-minute aggregations with watermarking
- **Alerts**: Real-time anomaly detection (HIGH_FLOW, LOW_BATTERY, HIGH_TEMP)
- **Storage**: Results indexed in Elasticsearch

### Visualization
- **Real-time**: Kibana dashboards for speed layer data
- **Historical**: Tableau reports from Hive tables

## How to Run

### Step 1: Start Pipeline
```bash
./run_pipeline_only.sh
```
- Starts Docker services (Kafka, Elasticsearch, HDFS)
- Initializes both Speed Layer (real-time) and Batch Layer (hourly)
- Sets up all processing components

### Step 2: Generate Data
```bash
./run_data_generator.sh
```
- Simulates IoT water meter readings
- Sends data to Kafka for processing
- Data flows through both real-time and batch processing

### Access Points
- **Real-time Dashboards**: http://localhost:5601 (Kibana)
- **Kafka Messages**: http://localhost:8080 (Kafka UI)
- **HDFS Storage**: http://localhost:9870 (NameNode)

### Tableau Integration
```bash
# Export batch processed data for Tableau
python3 visualization/tableau_connectors.py
```
**Generates:**
- `tableau_daily_stats.csv` - Daily consumption patterns
- `tableau_monthly_stats.csv` - Monthly trends and forecasting
- `tableau_problem_meters.csv` - Maintenance and anomaly reports

**Direct Connection:** Hive Server at localhost:10000

## Project Structure
```
├── data/
│   ├── Digital Meter Data - July 2022/  # Real CSV data
│   └── managedobject_details.csv        # Meter metadata
├── data_generator/
│   ├── water_meter_simulator.py         # Raw IoT data generator
│   ├── demo_real_data.py               # Demo script
│   └── README.md                       # Generator docs
├── lambda_architecture/
│   ├── data_enrichment.py              # Metadata enrichment
│   ├── data_ingestion.py               # Kafka → HDFS + enrichment
│   ├── batch_layer/
│   │   ├── batch_processor.py          # Spark batch jobs
│   │   └── hive_processor.py           # Hive data warehouse
│   └── speed_layer/
│       └── stream_processor.py         # Spark streaming
├── visualization/
│   ├── elasticsearch_mapping.py        # ES batch index
│   ├── elasticsearch_speed_mapping_updated.py  # ES speed index
│   ├── kibana_dashboard_updated.json   # Kibana config
│   └── tableau_connectors.py           # Tableau integration
└── docker-compose.yml                      # Infrastructure
```

## Data Processing
- **2,817 IoT meters** across Queensland suburbs
- **~5.2M readings/month** with multiple measurement types
- **Real-time anomaly detection** for maintenance alerts
- **Historical trend analysis** for consumption patterns