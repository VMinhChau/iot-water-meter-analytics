# IoT Water Meter Analytics - Lambda Architecture

## Overview
Real-time analytics system processing actual IoT water meter data using Lambda Architecture with Kafka, Spark, Delta Lake, and Elasticsearch.

## Lambda Architecture Data Flow
```
IoT Meters → Kafka → ┌─ Spark Streaming (Speed) → Elasticsearch → Kibana (Real-time)
                     │
                     └─ Spark Streaming (Ingestion) → Delta Lake (Raw Data)
                                                        │
                                                        ↓
                                              Spark Batch → Delta Lake (Processed) → Spark Thrift → Tableau
```

### Speed Layer (Real-time Processing)
- ⚡ **Spark Streaming** - 5-minute sliding windows with watermarking
- 🔍 **Elasticsearch** - Real-time indexing for stats and alerts
- 📊 **Kibana** - Live dashboards and monitoring
- 🚨 **Real-time alerts** - Anomaly detection (high flow, low battery, leaks)

### Batch Layer (Historical Processing)
- 🕐 **2-minute trigger** - Kafka to Delta Lake ingestion (raw data storage)
- 💾 **Delta Lake** - ACID transactions with time travel
- 🗄️ **Every 2 minutes** - Batch processing for aggregations
- 📈 **Tableau** - Historical reports via JDBC connection

## Data Schema

### Raw IoT Data (from meters):
```json
{
  "meter_id": "83008",
  "timestamp": "2022-07-13T07:30:01.000Z",
  "measurement_type": "Pulse1",
  "series": "P1",
  "unit": "L",
  "value": 6.0
}
```

### Enriched Data (stored in Delta Lake):
```json
{
  "meter_id": "83008",
  "timestamp": "2022-07-13T07:30:01.000Z",
  "measurement_type": "Pulse1",
  "series": "P1",
  "unit": "L",
  "value": 6.0,
  "Meter Type": "captis_pulse",
  "Suburb": "BUDERIM",
  "Postcode": "4556",
  "Usage Type": "Residential",
  "date": "2022-07-13"
}
```

## Implementation Components

### Data Ingestion (Batch Layer)
- **Raw Data**: IoT meters send sensor readings to Kafka
- **Batch Ingestion**: Spark Streaming reads from Kafka and writes raw data to Delta Lake (HDFS)
- **Storage**: Raw data stored in Delta Lake at `hdfs://namenode:8020/data/water_meter/raw_delta/`

### Batch Processing
- **Processing**: Spark reads raw data from Delta Lake every 2 minutes
- **Enrichment**: Joins with meter metadata from HDFS for location/type information
- **Aggregations**: Daily/monthly stats by measurement_type and location
- **Anomaly Detection**: Identifies problem meters (high consumption, low battery, high temperature)
- **Storage**: Processed results stored back to Delta Lake in separate paths (daily_stats, monthly_stats, problem_meters)

### Speed Layer
- **Real-time Processing**: Spark Streaming processes Kafka data with 5-minute windows
- **Enrichment**: Real-time metadata joins using broadcast variables
- **Anomaly Detection**: HIGH_FLOW, LOW_BATTERY, HIGH_TEMP, LEAK_SUSPECT alerts
- **Storage**: Stats and alerts indexed in Elasticsearch
- **Note**: Speed layer is currently commented out in orchestrator but code exists

### Visualization
- **Real-time**: Kibana dashboards for speed layer data
- **Historical**: Tableau connects via Spark Thrift Server (JDBC)

## How to Run

### Step 1: Start Pipeline
```bash
./run_pipeline_only.sh
```
- Starts Docker services (Kafka, Elasticsearch, HDFS)
- Initializes both Speed Layer (real-time) and Batch Layer (every 2 minutes)
- Sets up all processing components

### Step 2: Generate Data
```bash
./run_data_generator.sh
```
- Simulates IoT water meter readings from real CSV data
- Sends data to Kafka for processing
- Data flows through both real-time and batch processing

### Step 4: Evaluate System (Optional)
```bash
./evaluate_system.sh
```
- Comprehensive performance evaluation
- Function testing and validation
- Automated benchmark testing
- Generates detailed reports in `performance/` folder

### Step 5: Reset (Optional)
```bash
./reset_spark_checkpoints.sh
```
- Clears Spark checkpoints if needed
- Use when restarting pipeline with fresh state

### Access Points
- **Real-time Dashboards**: http://localhost:5601 (Kibana)
- **Kafka Messages**: http://localhost:8080 (Kafka UI)
- **HDFS Storage**: http://localhost:9870 (NameNode)
- **Spark Master**: http://localhost:8081 (Spark UI)
- **Spark Thrift Server**: http://localhost:10000 (JDBC for Tableau)

### Tableau Integration
```bash
# Export batch processed data for Tableau
python visualization/tableau_connectors.py
```
**Generates:**
- `tableau_daily_stats.csv` - Daily consumption patterns
- `tableau_monthly_stats.csv` - Monthly trends and forecasting
- `tableau_problem_meters.csv` - Maintenance and anomaly reports

**Direct Connection:** Spark Thrift Server at localhost:10000 (JDBC)

## Project Structure
```
├── data/
│   ├── Digital Meter Data - July 2022/  # Real CSV data (31 daily files)
│   └── managedobject_details.csv        # Meter metadata
├── data_generator/
│   └── water_meter_simulator.py         # IoT data generator
├── kafka_setup/
│   └── kafka_config.py                 # Kafka topic setup
├── lambda_architecture/
│   ├── config.py                       # Configuration settings
│   ├── orchestrator.py                 # Main pipeline orchestrator
│   ├── unified_spark_ingestion.py      # Kafka → Delta Lake ingestion
│   ├── data_enrichment.py              # Metadata enrichment
│   ├── data_cleaning.py                # Data quality utilities
│   ├── batch_layer/
│   │   └── batch_processor.py          # Batch processing jobs
│   └── speed_layer/
│       └── stream_processor.py         # Real-time streaming
├── visualization/
│   ├── elasticsearch_speed_mapping_updated.py  # ES index setup (for speed layer)
│   └── tableau_connectors.py           # Tableau data export
├── docker-compose.yml                  # Infrastructure services
├── run_pipeline_only.sh               # Start pipeline script
├── run_data_generator.sh              # Start data generator
└── reset_spark_checkpoints.sh         # Reset Spark checkpoints
```

## Data Processing
- **2,817 IoT meters** across Queensland suburbs
- **~5.2M readings/month** with multiple measurement types (Pulse1, Battery, Temperature)
- **Real-time anomaly detection** for maintenance alerts and leak detection
- **Historical trend analysis** with Delta Lake time travel capabilities
- **ACID transactions** ensuring data consistency and reliability