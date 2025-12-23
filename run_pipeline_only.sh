#!/bin/bash

set -e  # Exit on any error

echo "🚀 Starting Data Pipeline Only..."
echo "================================"

# Install dependencies
echo "📦 Installing dependencies..."
pip install -r requirements.txt

# Start infrastructure
echo "🐳 Starting Docker services..."
docker-compose up -d

# Wait for services
echo "⏳ Waiting for services (30s)..."
sleep 30

# Setup Spark environment
echo "⚙️ Setting up Spark environment..."
docker exec -u 0 spark-master mkdir -p /home/spark/.ivy2/cache
docker exec -u 0 spark-master chown -R 185:185 /home/spark

# Setup HDFS directories and permissions
echo "🗄️ Setting up HDFS directories and permissions..."
docker exec namenode hdfs dfs -mkdir -p /data/water_meter 2>/dev/null || true
docker exec namenode hdfs dfs -chmod -R 777 /data/water_meter 2>/dev/null || true

# Setup Kafka topics
echo "📡 Setting up Kafka topics..."
python kafka_setup/kafka_config.py

# Setup Elasticsearch
echo "🔍 Setting up Elasticsearch..."
python visualization/elasticsearch_speed_mapping_updated.py

echo "✅ Infrastructure ready!"
echo ""
echo "🎯 Starting Processing Pipeline..."

# Start orchestrator (processing only)
python lambda_architecture/orchestrator.py &
ORCHESTRATOR_PID=$!

echo ""
echo "🎉 Pipeline Ready for Data!"
echo "📊 Kibana: http://localhost:5601"
echo "📈 Kafka UI: http://localhost:8080"
echo "💾 HDFS: http://localhost:9870"
echo "⚡ Spark Master: http://localhost:8081"
echo ""
echo "💡 To generate data: ./run_data_generator.sh"
echo "🔄 To reset checkpoints: ./reset_spark_checkpoints.sh"
echo "Press Ctrl+C to stop pipeline..."

# Cleanup function
cleanup() {
    echo ""
    echo "🛑 Stopping pipeline..."
    kill $ORCHESTRATOR_PID 2>/dev/null || true
    docker-compose down
    echo "✅ Pipeline stopped"
    exit 0
}

# Set trap for cleanup
trap cleanup INT TERM

# Wait for process
wait