import requests
import json

ES_HOST = "http://localhost:9200"
INDEX_NAME = "water-meter-index"

mapping = {
    "mappings": {
        "properties": {
            "meter_id": {"type": "keyword"},
            "timestamp": {"type": "date"},
            "measurement_type": {"type": "keyword"},
            "value": {"type": "float"},
            "pulse_value": {"type": "long"},
            "battery_level": {"type": "float"},
            "temperature": {"type": "float"},
            "raw_payload": {"type": "keyword"},
        }
    }
}

def delete_existing_index():
    url = f"{ES_HOST}/{INDEX_NAME}"
    r = requests.delete(url)
    print("Delete index response:", r.text)

def create_index():
    url = f"{ES_HOST}/{INDEX_NAME}"
    headers = {"Content-Type": "application/json"}
    r = requests.put(url, headers=headers, data=json.dumps(mapping))
    print("Create index response:", r.text)

if __name__ == "__main__":
    print("Setting up Elasticsearch index and mapping...")
    delete_existing_index()
    create_index()
    print("Elasticsearch index setup complete.")