from elasticsearch import Elasticsearch


class ElasticsearchSpeedSetup:
    def __init__(self, host: str = "http://localhost:9200") -> None:
        self.es = Elasticsearch(hosts=[host])

    def create_speed_index_mapping(self) -> None:
        """Create index mapping for speed layer data."""
        mapping = {
            "mappings": {
                "properties": {
                    "meter_id": {"type": "long"},
                    "window_start": {"type": "date"},
                    "window_end": {"type": "date"},
                    "measurement_type": {"type": "keyword"},
                    "total_value": {"type": "double"},
                    "avg_value": {"type": "double"},
                    "reading_count": {"type": "long"},
                    "suburb": {"type": "keyword"},
                    "meter_type": {"type": "keyword"},
                    "usage_type": {"type": "keyword"},
                }
            }
        }

        try:
            self.es.indices.create(index="water-meter-speed", body=mapping)
            print("Speed layer index created successfully")
        except Exception as exc:
            print(f"Speed index may already exist: {exc}")

    def create_alerts_index_mapping(self) -> None:
        """Create index mapping for alerts."""
        mapping = {
            "mappings": {
                "properties": {
                    "meter_id": {"type": "long"},
                    "timestamp": {"type": "date"},
                    "measurement_type": {"type": "keyword"},
                    "value": {"type": "double"},
                    "alert_type": {"type": "keyword"},
                    "suburb": {"type": "keyword"},
                    "meter_type": {"type": "keyword"},
                    "usage_type": {"type": "keyword"},
                    "severity": {"type": "keyword"},
                }
            }
        }

        try:
            self.es.indices.create(index="water-meter-alerts", body=mapping)
            print("Alerts index created successfully")
        except Exception as exc:
            print(f"Alerts index may already exist: {exc}")

    def index_sample_speed_data(self) -> None:
        """Index sample speed layer data."""
        sample_docs = [
            {
                "meter_id": 83008,
                "window_start": "2022-07-13T07:30:00.000Z",
                "window_end": "2022-07-13T07:35:00.000Z",
                "measurement_type": "Pulse1",
                "total_value": 15.0,
                "avg_value": 3.0,
                "reading_count": 5,
                "suburb": "BUDERIM",
                "meter_type": "captis_pulse",
                "usage_type": "Residential",
            }
        ]

        for doc in sample_docs:
            self.es.index(index="water-meter-speed", document=doc)

        print("Sample speed data indexed")

    def index_sample_alert_data(self) -> None:
        """Index sample alert data."""
        sample_alerts = [
            {
                "meter_id": 48281097,
                "timestamp": "2022-07-13T07:30:01.000Z",
                "measurement_type": "Pulse1",
                "value": 460.0,
                "alert_type": "HIGH_FLOW",
                "suburb": "MAROOCHYDORE",
                "meter_type": "captis_pulse",
                "usage_type": "Non-Residential",
                "severity": "WARNING",
            }
        ]

        for alert in sample_alerts:
            self.es.index(index="water-meter-alerts", document=alert)

        print("Sample alert data indexed")


def main() -> None:
    setup = ElasticsearchSpeedSetup()
    setup.create_speed_index_mapping()
    setup.create_alerts_index_mapping()
    setup.index_sample_speed_data()
    setup.index_sample_alert_data()


if __name__ == "__main__":
    main()