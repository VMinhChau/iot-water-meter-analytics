# How to Run IoT Water Meter Analytics

## 🚀 Lambda Architecture Pipeline

### Step 1: Start Pipeline
```bash
./run_pipeline_only.sh
```
**What it starts:**
- 🐳 Docker services (Kafka, Elasticsearch, HDFS, Spark)
- ⚡ **Speed Layer** - Real-time stream processing (5-minute windows)
- 📊 **Batch Layer** - Hourly historical processing
- 📥 **Data Ingestion** - Kafka → HDFS storage with enrichment

### Step 2: Generate Data
```bash
./run_data_generator.sh
```
**What it does:**
- 🌊 Simulates 2,817 IoT water meters
- 📈 Generates ~5.2M readings/month
- 🚨 Triggers real-time anomaly detection

## 🌐 Access Points

- **Real-time Dashboards**: http://localhost:5601 (Kibana - Speed Layer)
- **Kafka Messages**: http://localhost:8080 (Data flow monitoring)
- **HDFS Storage**: http://localhost:9870 (Batch Layer storage)
- **Elasticsearch**: http://localhost:9200 (Real-time data)

## 📊 Lambda Architecture Data Flow
```
IoT Meters → Kafka → ┌─ Speed Layer → Elasticsearch → Kibana (Real-time)
                     │
                     └─ Batch Layer → HDFS → Hive → Tableau (Historical)
```

### ⚡ Speed Layer (Real-time)
- **Processing**: Spark Streaming with 5-minute windows
- **Storage**: Elasticsearch for immediate indexing
- **Visualization**: Kibana dashboards
- **Use Cases**: Live monitoring, instant alerts, operational dashboards

### 📊 Batch Layer (Historical)
- **Processing**: Hourly Spark jobs for deep analysis
- **Storage**: HDFS (raw data) + Hive (data warehouse)
- **Visualization**: Tableau reports and analytics
- **Use Cases**: Historical trends, business reports, forecasting

### 📈 Tableau Integration
```bash
# Export processed batch data
python3 visualization/tableau_connectors.py
```
**Outputs**: Daily stats, monthly trends, problem meter reports

## 🛑 To Stop

Press `Ctrl+C` in each terminal - graceful shutdown included

## 🎯 Features

- ✅ **Dual processing** - Real-time (Speed) + Historical (Batch) layers
- ✅ **Live dashboards** - Kibana for operational monitoring
- ✅ **Business analytics** - Tableau for historical insights
- ✅ **Anomaly detection** - Instant alerts + trend analysis
- ✅ **Scalable** - Handles 5.2M+ readings/month
- ✅ **Production ready** - Fault tolerance and monitoring