#!/usr/bin/env python3
"""
Performance Evaluation Module for IoT Water Meter Analytics
Đánh giá hiệu suất và chức năng của hệ thống Lambda Architecture
"""

import time
import json
import psutil
import pandas as pd
from datetime import datetime, timedelta
from kafka import KafkaProducer, KafkaConsumer
from elasticsearch import Elasticsearch
import requests
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor
import numpy as np
from benchmark import PerformanceEvaluator as BenchmarkEvaluator

class PerformanceEvaluator:
    def __init__(self):
        self.results = {}
        self.start_time = None
        self.es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
        
    def evaluate_all(self):
        """Chạy tất cả các đánh giá"""
        print("🔍 Bắt đầu đánh giá hiệu suất hệ thống...")
        
        # 1. Đánh giá Infrastructure
        self.evaluate_infrastructure()
        
        # 2. Đánh giá Kafka Performance
        self.evaluate_kafka_performance()
        
        # 3. Đánh giá Spark Processing
        self.evaluate_spark_performance()
        
        # 4. Đánh giá Elasticsearch
        self.evaluate_elasticsearch_performance()
        
        # 5. Đánh giá End-to-End Latency
        self.evaluate_end_to_end_latency()
        
        # 6. Đánh giá Data Quality
        self.evaluate_data_quality()
        
        # 7. Benchmark Testing
        self.run_benchmark_tests()
        
        # 8. Tạo báo cáo
        self.generate_report()
        
    def evaluate_infrastructure(self):
        """Đánh giá tài nguyên hệ thống"""
        print("📊 Đánh giá Infrastructure...")
        
        # CPU và Memory
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        # Docker containers
        containers = subprocess.run(['docker', 'ps', '--format', 'table {{.Names}}\t{{.Status}}'], 
                                  capture_output=True, text=True)
        
        self.results['infrastructure'] = {
            'cpu_usage': cpu_percent,
            'memory_usage': memory.percent,
            'memory_available_gb': memory.available / (1024**3),
            'disk_usage': disk.percent,
            'disk_free_gb': disk.free / (1024**3),
            'containers_status': containers.stdout,
            'timestamp': datetime.now().isoformat()
        }
        
    def evaluate_kafka_performance(self):
        """Đánh giá hiệu suất Kafka"""
        print("📡 Đánh giá Kafka Performance...")
        
        # Test throughput
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        # Gửi test messages
        test_messages = 1000
        start_time = time.time()
        
        for i in range(test_messages):
            test_data = {
                'meter_id': f'test_{i}',
                'timestamp': datetime.now().isoformat(),
                'value': np.random.uniform(0, 100),
                'test_id': i
            }
            producer.send('water_meter_data', test_data)
        
        producer.flush()
        send_time = time.time() - start_time
        
        self.results['kafka'] = {
            'throughput_msg_per_sec': test_messages / send_time,
            'send_latency_ms': (send_time / test_messages) * 1000,
            'test_messages_sent': test_messages,
            'timestamp': datetime.now().isoformat()
        }
        
        producer.close()
        
    def evaluate_spark_performance(self):
        """Đánh giá hiệu suất Spark"""
        print("⚡ Đánh giá Spark Performance...")
        
        try:
            # Spark Master UI
            spark_ui = requests.get('http://localhost:8081/json/')
            spark_data = spark_ui.json()
            
            # Spark Applications
            apps_response = requests.get('http://localhost:8081/json/applications/')
            apps_data = apps_response.json()
            
            self.results['spark'] = {
                'master_status': spark_data.get('status', 'Unknown'),
                'workers_count': len(spark_data.get('workers', [])),
                'active_apps': len([app for app in apps_data if app.get('state') == 'RUNNING']),
                'total_cores': sum([worker.get('cores', 0) for worker in spark_data.get('workers', [])]),
                'total_memory_mb': sum([worker.get('memory', 0) for worker in spark_data.get('workers', [])]),
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.results['spark'] = {
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
            
    def evaluate_elasticsearch_performance(self):
        """Đánh giá hiệu suất Elasticsearch"""
        print("🔍 Đánh giá Elasticsearch Performance...")
        
        try:
            # Cluster health
            health = self.es.cluster.health()
            
            # Index stats
            stats = self.es.indices.stats()
            
            # Test indexing performance
            test_docs = 100
            start_time = time.time()
            
            for i in range(test_docs):
                doc = {
                    'meter_id': f'perf_test_{i}',
                    'timestamp': datetime.now().isoformat(),
                    'value': np.random.uniform(0, 100),
                    'test_type': 'performance_evaluation'
                }
                self.es.index(index='water_meter_stats', body=doc)
            
            index_time = time.time() - start_time
            
            self.results['elasticsearch'] = {
                'cluster_status': health['status'],
                'number_of_nodes': health['number_of_nodes'],
                'active_shards': health['active_shards'],
                'indexing_rate_docs_per_sec': test_docs / index_time,
                'avg_indexing_latency_ms': (index_time / test_docs) * 1000,
                'total_indices': len(stats['indices']),
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.results['elasticsearch'] = {
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
            
    def evaluate_end_to_end_latency(self):
        """Đánh giá độ trễ end-to-end"""
        print("🎯 Đánh giá End-to-End Latency...")
        
        # Gửi test message với timestamp
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        test_id = f"latency_test_{int(time.time())}"
        send_time = time.time()
        
        test_message = {
            'meter_id': test_id,
            'timestamp': datetime.now().isoformat(),
            'value': 99.99,
            'test_type': 'latency_test',
            'send_timestamp': send_time
        }
        
        producer.send('water_meter_data', test_message)
        producer.flush()
        producer.close()
        
        # Chờ và kiểm tra trong Elasticsearch
        time.sleep(10)  # Chờ processing
        
        try:
            # Tìm message trong ES
            search_result = self.es.search(
                index='water_meter_stats',
                body={
                    'query': {
                        'term': {'meter_id': test_id}
                    }
                }
            )
            
            if search_result['hits']['total']['value'] > 0:
                receive_time = time.time()
                latency = (receive_time - send_time) * 1000
                
                self.results['end_to_end_latency'] = {
                    'latency_ms': latency,
                    'status': 'success',
                    'timestamp': datetime.now().isoformat()
                }
            else:
                self.results['end_to_end_latency'] = {
                    'status': 'message_not_found',
                    'timestamp': datetime.now().isoformat()
                }
                
        except Exception as e:
            self.results['end_to_end_latency'] = {
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
            
    def evaluate_data_quality(self):
        """Đánh giá chất lượng dữ liệu"""
        print("📈 Đánh giá Data Quality...")
        
        try:
            # Kiểm tra dữ liệu trong ES
            search_result = self.es.search(
                index='water_meter_stats',
                body={
                    'size': 1000,
                    'query': {'match_all': {}}
                }
            )
            
            docs = search_result['hits']['hits']
            
            if docs:
                # Phân tích chất lượng
                total_docs = len(docs)
                missing_fields = 0
                invalid_values = 0
                
                for doc in docs:
                    source = doc['_source']
                    
                    # Kiểm tra missing fields
                    required_fields = ['meter_id', 'timestamp', 'value']
                    if not all(field in source for field in required_fields):
                        missing_fields += 1
                    
                    # Kiểm tra invalid values
                    if 'value' in source:
                        try:
                            value = float(source['value'])
                            if value < 0 or value > 10000:  # Reasonable range
                                invalid_values += 1
                        except:
                            invalid_values += 1
                
                self.results['data_quality'] = {
                    'total_documents': total_docs,
                    'missing_fields_count': missing_fields,
                    'invalid_values_count': invalid_values,
                    'data_completeness_percent': ((total_docs - missing_fields) / total_docs) * 100,
                    'data_validity_percent': ((total_docs - invalid_values) / total_docs) * 100,
                    'timestamp': datetime.now().isoformat()
                }
            else:
                self.results['data_quality'] = {
                    'status': 'no_data_found',
                    'timestamp': datetime.now().isoformat()
                }
                
        except Exception as e:
            self.results['data_quality'] = {
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
            
    def generate_report(self):
        """Tạo báo cáo đánh giá"""
        print("📋 Tạo báo cáo đánh giá...")
        
        report = {
            'evaluation_timestamp': datetime.now().isoformat(),
            'system_performance': self.results,
            'summary': self._generate_summary()
        }
        
        # Lưu báo cáo JSON
        with open('performance_report.json', 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        # Tạo báo cáo readable
        self._create_readable_report(report)
        
        print("✅ Báo cáo đã được lưu:")
        print("   📄 performance_report.json")
        print("   📊 performance_summary.txt")
        
    def _generate_summary(self):
        """Tạo tóm tắt đánh giá"""
        summary = {}
        
        # Infrastructure Summary
        if 'infrastructure' in self.results:
            infra = self.results['infrastructure']
            summary['infrastructure_health'] = 'Good' if infra['cpu_usage'] < 80 and infra['memory_usage'] < 80 else 'Warning'
        
        # Kafka Summary
        if 'kafka' in self.results:
            kafka = self.results['kafka']
            summary['kafka_performance'] = 'Good' if kafka.get('throughput_msg_per_sec', 0) > 100 else 'Needs Improvement'
        
        # Spark Summary
        if 'spark' in self.results:
            spark = self.results['spark']
            summary['spark_status'] = spark.get('master_status', 'Unknown')
        
        # Elasticsearch Summary
        if 'elasticsearch' in self.results:
            es = self.results['elasticsearch']
            summary['elasticsearch_health'] = es.get('cluster_status', 'Unknown')
        
        # End-to-End Latency Summary
        if 'end_to_end_latency' in self.results:
            latency = self.results['end_to_end_latency']
            if 'latency_ms' in latency:
                summary['latency_performance'] = 'Excellent' if latency['latency_ms'] < 1000 else 'Good' if latency['latency_ms'] < 5000 else 'Needs Improvement'
        
        # Data Quality Summary
        if 'data_quality' in self.results:
            quality = self.results['data_quality']
            if 'data_completeness_percent' in quality:
                summary['data_quality'] = 'Excellent' if quality['data_completeness_percent'] > 95 else 'Good' if quality['data_completeness_percent'] > 90 else 'Needs Improvement'
        
        return summary
        
    def _create_readable_report(self, report):
        """Tạo báo cáo dễ đọc"""
        with open('performance_summary.txt', 'w', encoding='utf-8') as f:
            f.write("🔍 BÁO CÁO ĐÁNH GIÁ HIỆU SUẤT HỆ THỐNG\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Thời gian đánh giá: {report['evaluation_timestamp']}\n\n")
            
            # Summary
            f.write("📊 TÓM TẮT TỔNG QUAN\n")
            f.write("-" * 20 + "\n")
            for key, value in report['summary'].items():
                f.write(f"{key.replace('_', ' ').title()}: {value}\n")
            f.write("\n")
            
            # Chi tiết từng component
            for component, data in report['system_performance'].items():
                f.write(f"🔧 {component.upper()}\n")
                f.write("-" * 20 + "\n")
                for key, value in data.items():
                    if key != 'timestamp':
                        f.write(f"{key}: {value}\n")
                f.write("\n")

if __name__ == "__main__":
    evaluator = PerformanceEvaluator()
    evaluator.evaluate_all()
            
    def run_benchmark_tests(self):
        """Chạy benchmark tests từ file benchmark.py"""
        print("🏃 Chạy Benchmark Tests...")
        
        try:
            benchmark = BenchmarkEvaluator()
            
            # Test throughput (30s)
            print("  📊 Testing throughput...")
            throughput = benchmark.measure_throughput(topic='water_meter_data', duration=30)
            
            # Test latency (50 samples)
            print("  ⏱️ Testing latency...")
            latency_stats = benchmark.measure_latency(samples=50)
            
            # Generate benchmark report
            benchmark_report = benchmark.generate_report()
            
            self.results['benchmark'] = {
                'throughput_msg_per_sec': throughput,
                'latency_stats': latency_stats,
                'full_benchmark_report': benchmark_report,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.results['benchmark'] = {
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }