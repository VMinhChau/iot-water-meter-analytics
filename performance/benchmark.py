import time
import psutil
import json
from kafka import KafkaConsumer
from datetime import datetime

class PerformanceEvaluator:
    def __init__(self):
        self.metrics = {
            'throughput': [],
            'latency': [],
            'cpu_usage': [],
            'memory_usage': [],
            'kafka_lag': []
        }
    
    def measure_throughput(self, topic='water-meter-readings', duration=60):
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers='localhost:9092',
            auto_offset_reset='latest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        start_time = time.time()
        message_count = 0
        
        while time.time() - start_time < duration:
            messages = consumer.poll(timeout_ms=1000)
            for tp, msgs in messages.items():
                message_count += len(msgs)
        
        throughput = message_count / duration
        self.metrics['throughput'].append(throughput)
        consumer.close()
        
        return throughput
    
    def measure_system_resources(self, duration=60):
        start_time = time.time()
        
        while time.time() - start_time < duration:
            cpu_percent = psutil.cpu_percent(interval=1)
            memory_percent = psutil.virtual_memory().percent
            
            self.metrics['cpu_usage'].append(cpu_percent)
            self.metrics['memory_usage'].append(memory_percent)
            
            time.sleep(5)
    
    def measure_latency(self, samples=100):
        consumer = KafkaConsumer(
            'water-meter-readings',
            bootstrap_servers='localhost:9092',
            auto_offset_reset='latest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        latencies = []
        count = 0
        
        for message in consumer:
            if count >= samples:
                break
            
            msg_timestamp = datetime.fromisoformat(message.value['timestamp'])
            current_time = datetime.now()
            latency = (current_time - msg_timestamp).total_seconds() * 1000
            
            latencies.append(latency)
            count += 1
        
        consumer.close()
        self.metrics['latency'] = latencies
        
        return {
            'avg_latency': sum(latencies) / len(latencies),
            'max_latency': max(latencies),
            'min_latency': min(latencies)
        }
    
    def generate_report(self):
        report = {
            'timestamp': datetime.now().isoformat(),
            'performance_metrics': {
                'avg_throughput': sum(self.metrics['throughput']) / len(self.metrics['throughput']) if self.metrics['throughput'] else 0,
                'avg_cpu_usage': sum(self.metrics['cpu_usage']) / len(self.metrics['cpu_usage']) if self.metrics['cpu_usage'] else 0,
                'avg_memory_usage': sum(self.metrics['memory_usage']) / len(self.metrics['memory_usage']) if self.metrics['memory_usage'] else 0,
                'latency_stats': {
                    'avg': sum(self.metrics['latency']) / len(self.metrics['latency']) if self.metrics['latency'] else 0,
                    'max': max(self.metrics['latency']) if self.metrics['latency'] else 0,
                    'min': min(self.metrics['latency']) if self.metrics['latency'] else 0
                }
            }
        }
        
        with open('performance_report.json', 'w') as f:
            json.dump(report, f, indent=2)
        
        return report

if __name__ == "__main__":
    evaluator = PerformanceEvaluator()
    
    print("Measuring throughput...")
    throughput = evaluator.measure_throughput()
    print(f"Throughput: {throughput:.2f} messages/second")
    
    print("Measuring latency...")
    latency_stats = evaluator.measure_latency()
    print(f"Latency - Avg: {latency_stats['avg_latency']:.2f}ms")
    
    report = evaluator.generate_report()
    print("Performance report generated")