"""
Connection pooling for external services
"""
import threading
import time
from queue import Queue, Empty
from elasticsearch import Elasticsearch
from kafka import KafkaProducer
import logging

logger = logging.getLogger(__name__)

class ConnectionPool:
    """Generic connection pool implementation"""
    
    def __init__(self, create_connection_func, max_connections=10, timeout=30):
        self.create_connection = create_connection_func
        self.max_connections = max_connections
        self.timeout = timeout
        self.pool = Queue(maxsize=max_connections)
        self.active_connections = 0
        self.lock = threading.Lock()
        
    def get_connection(self):
        """Get connection from pool or create new one"""
        try:
            # Try to get existing connection
            connection = self.pool.get_nowait()
            return connection
        except Empty:
            # Create new connection if under limit
            with self.lock:
                if self.active_connections < self.max_connections:
                    connection = self.create_connection()
                    self.active_connections += 1
                    return connection
                else:
                    # Wait for available connection
                    return self.pool.get(timeout=self.timeout)
    
    def return_connection(self, connection):
        """Return connection to pool"""
        try:
            self.pool.put_nowait(connection)
        except:
            # Pool is full, close connection
            self._close_connection(connection)
            with self.lock:
                self.active_connections -= 1
    
    def _close_connection(self, connection):
        """Close connection (override in subclasses)"""
        if hasattr(connection, 'close'):
            connection.close()

class ElasticsearchPool(ConnectionPool):
    """Elasticsearch connection pool"""
    
    def __init__(self, hosts, max_connections=5, **es_kwargs):
        def create_es_connection():
            return Elasticsearch(hosts, **es_kwargs)
        
        super().__init__(create_es_connection, max_connections)
        self.hosts = hosts
        
    def execute_bulk(self, actions):
        """Execute bulk operations with pooled connection"""
        connection = self.get_connection()
        try:
            from elasticsearch.helpers import bulk
            return bulk(connection, actions)
        finally:
            self.return_connection(connection)

class KafkaProducerPool(ConnectionPool):
    """Kafka producer connection pool"""
    
    def __init__(self, bootstrap_servers, max_connections=3, **kafka_kwargs):
        def create_kafka_producer():
            return KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                acks='all',
                retries=3,
                enable_idempotence=True,
                **kafka_kwargs
            )
        
        super().__init__(create_kafka_producer, max_connections)
        
    def send_message(self, topic, value, key=None):
        """Send message with pooled producer"""
        producer = self.get_connection()
        try:
            future = producer.send(topic, value=value, key=key)
            producer.flush()  # Ensure message is sent
            return future
        finally:
            self.return_connection(producer)
    
    def _close_connection(self, connection):
        """Close Kafka producer"""
        connection.close()

# Global connection pools
_es_pool = None
_kafka_pool = None

def get_elasticsearch_pool(hosts="localhost:9200", max_connections=5):
    """Get or create Elasticsearch connection pool"""
    global _es_pool
    if _es_pool is None:
        _es_pool = ElasticsearchPool([hosts], max_connections)
    return _es_pool

def get_kafka_producer_pool(bootstrap_servers="localhost:9092", max_connections=3):
    """Get or create Kafka producer connection pool"""
    global _kafka_pool
    if _kafka_pool is None:
        _kafka_pool = KafkaProducerPool(bootstrap_servers, max_connections)
    return _kafka_pool