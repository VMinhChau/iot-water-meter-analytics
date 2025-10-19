import time
import psutil
import logging
from datetime import datetime
from typing import Dict, Any
from config import Config

logger = logging.getLogger(__name__)

class PerformanceMonitor:
    def __init__(self, config=None):
        self.config = config or Config()
        self.metrics_history = []
        self.start_time = time.time()
    
    def collect_system_metrics(self) -> Dict[str, Any]:
        """Collect system performance metrics"""
        return {
            "timestamp": datetime.now().isoformat(),
            "cpu_percent": psutil.cpu_percent(interval=1),
            "memory_percent": psutil.virtual_memory().percent,
            "disk_usage": psutil.disk_usage('/').percent,
            "network_io": psutil.net_io_counters()._asdict(),
            "uptime_seconds": time.time() - self.start_time
        }
    
    def collect_processing_metrics(self, spark_session=None) -> Dict[str, Any]:
        """Collect Spark processing metrics"""
        metrics = {
            "timestamp": datetime.now().isoformat(),
            "active_jobs": 0,
            "completed_jobs": 0,
            "failed_jobs": 0
        }
        
        if spark_session:
            try:
                sc = spark_session.sparkContext
                status_tracker = sc.statusTracker()
                
                metrics.update({
                    "active_jobs": len(status_tracker.getActiveJobIds()),
                    "executor_infos": len(status_tracker.getExecutorInfos()),
                    "application_id": sc.applicationId
                })
            except Exception as e:
                logger.warning(f"Failed to collect Spark metrics: {e}")
        
        return metrics
    
    def calculate_throughput(self, records_processed: int, time_window: float) -> float:
        """Calculate processing throughput"""
        if time_window <= 0:
            return 0.0
        return records_processed / time_window
    
    def detect_performance_issues(self, metrics: Dict[str, Any]) -> list:
        """Detect performance bottlenecks"""
        issues = []
        
        # CPU usage check
        if metrics.get("cpu_percent", 0) > 80:
            issues.append("HIGH_CPU_USAGE")
        
        # Memory usage check
        if metrics.get("memory_percent", 0) > 85:
            issues.append("HIGH_MEMORY_USAGE")
        
        # Disk usage check
        if metrics.get("disk_usage", 0) > 90:
            issues.append("HIGH_DISK_USAGE")
        
        return issues
    
    def generate_performance_report(self, spark_session=None) -> Dict[str, Any]:
        """Generate comprehensive performance report"""
        system_metrics = self.collect_system_metrics()
        processing_metrics = self.collect_processing_metrics(spark_session)
        issues = self.detect_performance_issues(system_metrics)
        
        report = {
            "system_metrics": system_metrics,
            "processing_metrics": processing_metrics,
            "performance_issues": issues,
            "recommendations": self._get_recommendations(issues)
        }
        
        # Store in history
        self.metrics_history.append(report)
        
        # Keep only last 100 reports
        if len(self.metrics_history) > 100:
            self.metrics_history = self.metrics_history[-100:]
        
        return report
    
    def _get_recommendations(self, issues: list) -> list:
        """Get performance optimization recommendations"""
        recommendations = []
        
        if "HIGH_CPU_USAGE" in issues:
            recommendations.append("Consider reducing Spark executor cores or increasing cluster size")
        
        if "HIGH_MEMORY_USAGE" in issues:
            recommendations.append("Increase executor memory or optimize data caching strategy")
        
        if "HIGH_DISK_USAGE" in issues:
            recommendations.append("Clean up old checkpoint files and temporary data")
        
        return recommendations
    
    def log_metrics(self, spark_session=None):
        """Log current performance metrics"""
        report = self.generate_performance_report(spark_session)
        
        logger.info(f"System CPU: {report['system_metrics']['cpu_percent']:.1f}%")
        logger.info(f"System Memory: {report['system_metrics']['memory_percent']:.1f}%")
        
        if report['performance_issues']:
            logger.warning(f"Performance issues detected: {report['performance_issues']}")
            for rec in report['recommendations']:
                logger.info(f"Recommendation: {rec}")

if __name__ == "__main__":
    monitor = PerformanceMonitor()
    
    while True:
        try:
            monitor.log_metrics()
            time.sleep(60)  # Log every minute
        except KeyboardInterrupt:
            logger.info("Performance monitoring stopped")
            break
        except Exception as e:
            logger.error(f"Performance monitoring error: {e}")
            time.sleep(30)