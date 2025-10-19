import pandas as pd
import json
import os
import logging

logger = logging.getLogger(__name__)

class DataEnrichment:
    def __init__(self, data_path=None):
        self.data_path = data_path or self._get_default_data_path()
        self.meter_details = self.load_meter_details()
    
    def _get_default_data_path(self):
        """Get default data path relative to project root"""
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(os.path.dirname(current_dir))
        return os.path.join(project_root, 'data', 'managedobject_details.csv')
    
    def load_meter_details(self):
        """Load meter metadata for enrichment"""
        try:
            if not os.path.exists(self.data_path):
                logger.warning(f"Meter details file not found: {self.data_path}")
                return {}
            
            df = pd.read_csv(self.data_path)
            logger.info(f"Loaded {len(df)} meter details from {self.data_path}")
            return df.set_index('managedObjects_id').to_dict('index')
            
        except Exception as e:
            logger.error(f"Failed to load meter details: {e}")
            return {}
    
    def enrich_reading(self, raw_reading):
        """Enrich raw IoT data with meter metadata"""
        meter_id = raw_reading['meter_id']
        meter_info = self.meter_details.get(meter_id, {})
        
        enriched_reading = raw_reading.copy()
        enriched_reading.update({
            'meter_type': meter_info.get('Meter Type', 'unknown'),
            'suburb': meter_info.get('Suburb', 'unknown'),
            'postcode': meter_info.get('Postcode', 'unknown'),
            'usage_type': meter_info.get('Usage Type', 'unknown')
        })
        
        return enriched_reading

if __name__ == "__main__":
    enricher = DataEnrichment()
    
    # Example raw IoT reading
    raw_data = {
        'meter_id': 83008,
        'timestamp': '2022-07-13T07:30:01.000Z',
        'measurement_type': 'Pulse1',
        'series': 'P1',
        'unit': 'L',
        'value': 6.0
    }
    
    enriched = enricher.enrich_reading(raw_data)
    print("Raw IoT data:", json.dumps(raw_data, indent=2))
    print("Enriched data:", json.dumps(enriched, indent=2))