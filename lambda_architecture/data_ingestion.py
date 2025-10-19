try:
    from kafka import KafkaConsumer
except ImportError:
    KafkaConsumer = None
    
try:
    from hdfs3 import HDFileSystem
except ImportError:
    HDFileSystem = None
    
import json
from datetime import datetime
import threading
import logging
import os
import sys
import time

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from utils import retry_on_failure, CircuitBreaker
except ImportError as e:
    logging.warning(f"Import warning: {e}. Using fallback implementations.")
    
    # Fallback implementations
    def retry_on_failure(max_retries=3, delay=1.0, backoff=2.0):
        def decorator(func):
            def wrapper(*args, **kwargs):
                for attempt in range(max_retries):
                    try:
                        return func(*args, **kwargs)
                    except Exception as e:
                        if attempt == max_retries - 1:
                            raise
                        time.sleep(delay * (backoff ** attempt))
                return None
            return wrapper
        return decorator
    
    class CircuitBreaker:
        def __init__(self, failure_threshold=5, timeout=60):
            pass
        def call(self, func, *args, **kwargs):
            return func(*args, **kwargs)

logger = logging.getLogger(__name__)

class DataIngestion:
    def __init__(self, batch_size=1000, max_retries=3):
        self.batch_size = batch_size
        self.circuit_breaker = CircuitBreaker(failure_threshold=5, timeout=60)
        self.hdfs = None
        self.consumer = None
        self.fallback_mode = False
        
        self._initialize_services()
    
    def _initialize_services(self):
        """Initialize services with fallback handling"""
        try:
            from service_manager import ServiceManager
            services = ServiceManager.get_required_services()
            available = ServiceManager.wait_for_services(services, max_wait=30)
        except ImportError:
            logger.warning("ServiceManager not available - using direct connection")
            available = {'hdfs': HDFileSystem is not None, 'kafka': KafkaConsumer is not None}
            
        if available.get('hdfs', False) and HDFileSystem:
            try:
                self.hdfs = HDFileSystem(host='localhost', port=9870)
                self.hdfs.ls('/')  # Test connection
                logger.info("HDFS connection established")
            except Exception as e:
                logger.warning(f"HDFS connection failed: {e} - using local fallback")
                self.fallback_mode = True
        else:
            logger.warning("HDFS not available - using local fallback")
            self.fallback_mode = True
        
        if available.get('kafka', False) and KafkaConsumer:
            try:
                self.consumer = KafkaConsumer(
                    'water-meter-readings',
                    bootstrap_servers='localhost:9092',
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    auto_offset_reset='latest',
                    enable_auto_commit=False,  # Manual offset management
                    group_id='water-meter-ingestion',
                    max_poll_records=1000,
                    session_timeout_ms=30000,
                    heartbeat_interval_ms=10000,
                    fetch_min_bytes=1024,
                    fetch_max_wait_ms=500
                )
                logger.info("Kafka connection established")
            except Exception as e:
                logger.warning(f"Kafka connection failed: {e} - data ingestion disabled")
        else:
            logger.warning("Kafka not available - data ingestion disabled")
    
    @retry_on_failure(max_retries=3, delay=1.0)
    def store_to_hdfs(self, data_batch):
        """Store batch to HDFS with partitioning and validation"""
        if not data_batch:
            return
        
        # Validate data before storing
        valid_records = []
        for record in data_batch:
            if self._validate_record(record):
                valid_records.append(record)
            else:
                logger.warning(f"Invalid record skipped: {str(record)[:100]}...")
        
        if not valid_records:
            logger.warning("No valid records in batch")
            return
        
        # Secure partitioned storage with path validation
        date_str = datetime.now().strftime('%Y-%m-%d')
        hour_str = datetime.now().strftime('%H')
        timestamp = int(datetime.now().timestamp())
        
        # Validate path components to prevent path traversal
        if not all(c.isalnum() or c in '-_' for c in date_str.replace('-', '')):
            raise ValueError("Invalid date format")
        if not hour_str.isdigit() or not (0 <= int(hour_str) <= 23):
            raise ValueError("Invalid hour format")
            
        hdfs_path = f'/data/water_meter/raw/date={date_str}/hour={hour_str}/batch_{timestamp}.json'
        
        if self.hdfs and not self.fallback_mode:
            def write_to_hdfs():
                with self.hdfs.open(hdfs_path, 'wb') as f:
                    for record in valid_records:
                        f.write((json.dumps(record) + '\n').encode())
            
            self.circuit_breaker.call(write_to_hdfs)
            logger.info(f"Stored {len(valid_records)} records to HDFS: {hdfs_path}")
        else:
            # Fallback to local storage
            local_path = f"./data/fallback/{date_str}_{hour_str}_{int(datetime.now().timestamp())}.json"
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            with open(local_path, 'w') as f:
                for record in valid_records:
                    f.write(json.dumps(record) + '\n')
            
            logger.info(f"Stored {len(valid_records)} records to local fallback: {local_path}")
    
    def _validate_record(self, record):
        """Enhanced record validation with comprehensive checks"""
        required_fields = ['meter_id', 'timestamp', 'measurement_type', 'value']
        
        # Check required fields
        for field in required_fields:
            if field not in record or record[field] is None:
                return False
        
        # Enhanced value validation
        try:
            meter_id = int(record['meter_id'])
            value = float(record['value'])
            measurement_type = str(record['measurement_type'])
            
            # Validate meter_id range
            if meter_id <= 0 or meter_id > 999999:
                return False
            
            # Validate value ranges based on measurement type
            if measurement_type in ['Pulse1', 'Pulse1_Total']:
                return 0 <= value <= 100000  # Reasonable flow range
            elif measurement_type == 'Battery':
                return 0 <= value <= 5.0  # Battery voltage range
            elif measurement_type == 'DeviceTemperature':
                return -20 <= value <= 80  # Temperature range
            else:
                return value >= 0  # Generic positive value check
                
        except (ValueError, TypeError, KeyError):
            return False
    
    def ingest_data(self):
        """Main ingestion loop with improved error handling"""
        if not self.consumer:
            logger.error("No Kafka consumer available - cannot ingest data")
            return
        
        batch = []
        
        try:
            for message in self.consumer:
                try:
                    raw_data = message.value
                    batch.append(raw_data)
                    
                    if len(batch) >= self.batch_size:
                        self.store_to_hdfs(batch)
                        # Commit offsets after successful processing
                        self.consumer.commit()
                        batch = []
                        
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    continue
                    
        except KeyboardInterrupt:
            logger.info("Ingestion stopped by user")
        except Exception as e:
            logger.error(f"Ingestion failed: {e}")
            raise
        finally:
            # Store remaining batch
            if batch:
                self.store_to_hdfs(batch)
                self.consumer.commit()
    
    def start_ingestion(self):
        """Start ingestion in separate thread"""
        ingestion_thread = threading.Thread(target=self.ingest_data, name="DataIngestion")
        ingestion_thread.daemon = True
        ingestion_thread.start()
        logger.info("Data ingestion started")
        return ingestion_thread

if __name__ == "__main__":
    ingestion = DataIngestion()
    thread = ingestion.start_ingestion()
    thread.join()