import requests
import json
import time

def register_hdfs_sink():
    connector_name = "water-meter-hdfs-sink"
    connect_url = "http://localhost:8083/connectors"

    config = {
        "name": connector_name,
        "config": {
            "connector.class": "io.confluent.connect.hdfs.HdfsSinkConnector",
            "tasks.max": "1",
            "topics": "water-meter-readings",

            "hdfs.url": "hdfs://namenode:8020",
            "topics.dir": "/data/water_meter/raw",
            "logs.dir": "/data/water_meter/logs",

            "flush.size": "100",                  
            "rotate.interval.ms": "30000",      

            "format.class": "io.confluent.connect.hdfs.parquet.ParquetFormat",
            # "store.compression.codec": "gzip",

            "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
            "path.format": "YYYY/MM/dd",
            "locale": "en",
            "timezone": "UTC",
            "partition.duration.ms": "3600000",

            "time.extractor": "Record",
            "timestamp.field": "timestamp",

            "hdfs.authentication.kerberos": "false",

            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "key.converter.schemas.enable": "false",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "true"
        }
    }

    print("📡 Registering HDFS Sink Connector...")
    response = requests.post(connect_url, headers={"Content-Type": "application/json"}, data=json.dumps(config))

    if response.status_code == 201:
        print("✅ Connector registered successfully!")
    elif response.status_code == 409:
        print("⚠️ Connector already exists, updating instead...")
        update_url = f"{connect_url}/{connector_name}/config"
        response = requests.put(update_url, headers={"Content-Type": "application/json"}, data=json.dumps(config["config"]))
        print("🔄 Connector updated." if response.ok else f"❌ Failed to update: {response.text}")
    else:
        print(f"❌ Failed to register connector: {response.status_code} {response.text}")

def monitor_connector():
    try:
        while True:
            status_url = "http://localhost:8083/connectors/water-meter-hdfs-sink/status"
            try:
                r = requests.get(status_url)
                if r.ok:
                    status = r.json()
                    print(f"Connector state: {status['connector']['state']}")
                else:
                    print(f"⚠️ Failed to get status: {r.text}")
            except Exception as e:
                print(f"❌ Cannot connect to Kafka Connect: {e}")
            time.sleep(15)
    except KeyboardInterrupt:
        print("🛑 Monitoring stopped by user.")

if __name__ == "__main__":
    register_hdfs_sink()
    # monitor_connector()
