#!/bin/bash

echo "🌊 Starting IoT Data Generator..."
echo "================================"

# Check if Kafka is running
if ! curl -f http://localhost:8080 > /dev/null 2>&1; then
    echo "❌ Kafka not running! Start pipeline first:"
    echo "   ./run_pipeline_only.sh"
    exit 1
fi

echo "✅ Kafka detected - starting data generation..."
echo ""
echo "📊 Monitor data flow:"
echo "- Kafka UI: http://localhost:8080"
echo "- Kibana: http://localhost:5601"
echo ""
echo "Press Ctrl+C to stop data generation..."

# Start data generator
python data_generator/water_meter_simulator.py

curl -s http://localhost:8083/connectors/water-meter-hdfs-sink/status

curl -X POST http://localhost:8083/connectors/water-meter-hdfs-sink/tasks/0/restart