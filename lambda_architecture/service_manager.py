import logging
import socket
import time
from typing import Dict, Tuple

logger = logging.getLogger(__name__)

class ServiceManager:
    """Manage external service dependencies with fallbacks"""
    
    @staticmethod
    def check_service(host: str, port: int, timeout: int = 3) -> bool:
        """Check if service is available"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            result = sock.connect_ex((host, port))
            sock.close()
            return result == 0
        except:
            return False
    
    @staticmethod
    def wait_for_services(services: Dict[str, Tuple[str, int]], max_wait: int = 60) -> Dict[str, bool]:
        """Wait for services to be available"""
        results = {}
        start_time = time.time()
        
        for name, (host, port) in services.items():
            logger.info(f"Checking {name} at {host}:{port}...")
            
            while time.time() - start_time < max_wait:
                if ServiceManager.check_service(host, port):
                    logger.info(f"✓ {name} is available")
                    results[name] = True
                    break
                time.sleep(2)
            else:
                logger.warning(f"✗ {name} not available after {max_wait}s")
                results[name] = False
        
        return results
    
    @staticmethod
    def get_required_services() -> Dict[str, Tuple[str, int]]:
        """Get list of required services"""
        return {
            'kafka': ('localhost', 9092),
            'hdfs': ('localhost', 9870),
            'elasticsearch': ('localhost', 9200)
        }