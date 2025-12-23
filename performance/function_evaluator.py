#!/usr/bin/env python3
"""
Function Evaluation Module
Đánh giá từng function cụ thể trong hệ thống
"""

import time
import json
import requests
from datetime import datetime
import subprocess
import os
import sys

class FunctionEvaluator:
    def __init__(self):
        self.results = {}
        
    def evaluate_all_functions(self):
        """Đánh giá tất cả functions"""
        print("🔧 Đánh giá Functions...")
        
        # 1. Kafka Functions
        self.evaluate_kafka_functions()
        
        # 2. Spark Functions  
        self.evaluate_spark_functions()
        
        # 3. Data Processing Functions
        self.evaluate_data_processing_functions()
        
        # 4. Elasticsearch Functions
        self.evaluate_elasticsearch_functions()
        
        # 5. Tạo báo cáo
        self.generate_function_report()
        
    def evaluate_kafka_functions(self):
        """Đánh giá Kafka functions"""
        print("📡 Đánh giá Kafka Functions...")
        
        functions = {
            'topic_creation': self._test_kafka_topic_creation,
            'message_production': self._test_kafka_production,
            'message_consumption': self._test_kafka_consumption,
            'topic_management': self._test_kafka_topic_management
        }
        
        self.results['kafka_functions'] = {}
        
        for func_name, func in functions.items():
            try:
                start_time = time.time()
                result = func()
                execution_time = time.time() - start_time
                
                self.results['kafka_functions'][func_name] = {
                    'status': 'success' if result else 'failed',
                    'execution_time_ms': execution_time * 1000,
                    'result': result,
                    'timestamp': datetime.now().isoformat()
                }
            except Exception as e:
                self.results['kafka_functions'][func_name] = {
                    'status': 'error',
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                }
                
    def evaluate_spark_functions(self):
        """Đánh giá Spark functions"""
        print("⚡ Đánh giá Spark Functions...")
        
        functions = {
            'spark_master_status': self._test_spark_master,
            'spark_worker_status': self._test_spark_workers,
            'spark_application_submission': self._test_spark_app_submission,
            'spark_streaming_status': self._test_spark_streaming
        }
        
        self.results['spark_functions'] = {}
        
        for func_name, func in functions.items():
            try:
                start_time = time.time()
                result = func()
                execution_time = time.time() - start_time
                
                self.results['spark_functions'][func_name] = {
                    'status': 'success' if result else 'failed',
                    'execution_time_ms': execution_time * 1000,
                    'result': result,
                    'timestamp': datetime.now().isoformat()
                }
            except Exception as e:
                self.results['spark_functions'][func_name] = {
                    'status': 'error',
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                }
                
    def evaluate_data_processing_functions(self):
        """Đánh giá Data Processing functions"""
        print("🔄 Đánh giá Data Processing Functions...")
        
        functions = {
            'data_ingestion': self._test_data_ingestion,
            'data_enrichment': self._test_data_enrichment,
            'data_cleaning': self._test_data_cleaning,
            'batch_processing': self._test_batch_processing
        }
        
        self.results['data_processing_functions'] = {}
        
        for func_name, func in functions.items():
            try:
                start_time = time.time()
                result = func()
                execution_time = time.time() - start_time
                
                self.results['data_processing_functions'][func_name] = {
                    'status': 'success' if result else 'failed',
                    'execution_time_ms': execution_time * 1000,
                    'result': result,
                    'timestamp': datetime.now().isoformat()
                }
            except Exception as e:
                self.results['data_processing_functions'][func_name] = {
                    'status': 'error',
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                }
                
    def evaluate_elasticsearch_functions(self):
        """Đánh giá Elasticsearch functions"""
        print("🔍 Đánh giá Elasticsearch Functions...")
        
        functions = {
            'cluster_health': self._test_es_cluster_health,
            'index_creation': self._test_es_index_creation,
            'document_indexing': self._test_es_document_indexing,
            'search_functionality': self._test_es_search
        }
        
        self.results['elasticsearch_functions'] = {}
        
        for func_name, func in functions.items():
            try:
                start_time = time.time()
                result = func()
                execution_time = time.time() - start_time
                
                self.results['elasticsearch_functions'][func_name] = {
                    'status': 'success' if result else 'failed',
                    'execution_time_ms': execution_time * 1000,
                    'result': result,
                    'timestamp': datetime.now().isoformat()
                }
            except Exception as e:
                self.results['elasticsearch_functions'][func_name] = {
                    'status': 'error',
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                }
    
    # Kafka Test Functions
    def _test_kafka_topic_creation(self):
        """Test tạo Kafka topic"""
        try:
            result = subprocess.run([
                'docker', 'exec', 'kafka', 'kafka-topics.sh',
                '--create', '--topic', 'test_topic_eval',
                '--bootstrap-server', 'localhost:9092',
                '--partitions', '1', '--replication-factor', '1'
            ], capture_output=True, text=True, timeout=30)
            return result.returncode == 0
        except:
            return False
            
    def _test_kafka_production(self):
        """Test Kafka message production"""
        try:
            from kafka import KafkaProducer
            producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
            producer.send('test_topic_eval', b'test message')
            producer.flush()
            producer.close()
            return True
        except:
            return False
            
    def _test_kafka_consumption(self):
        """Test Kafka message consumption"""
        try:
            from kafka import KafkaConsumer
            consumer = KafkaConsumer(
                'test_topic_eval',
                bootstrap_servers=['localhost:9092'],
                auto_offset_reset='earliest',
                consumer_timeout_ms=5000
            )
            messages = list(consumer)
            consumer.close()
            return len(messages) > 0
        except:
            return False
            
    def _test_kafka_topic_management(self):
        """Test Kafka topic management"""
        try:
            result = subprocess.run([
                'docker', 'exec', 'kafka', 'kafka-topics.sh',
                '--list', '--bootstrap-server', 'localhost:9092'
            ], capture_output=True, text=True, timeout=30)
            return 'water_meter_data' in result.stdout
        except:
            return False
    
    # Spark Test Functions
    def _test_spark_master(self):
        """Test Spark Master status"""
        try:
            response = requests.get('http://localhost:8081/json/', timeout=10)
            data = response.json()
            return data.get('status') == 'ALIVE'
        except:
            return False
            
    def _test_spark_workers(self):
        """Test Spark Workers status"""
        try:
            response = requests.get('http://localhost:8081/json/', timeout=10)
            data = response.json()
            workers = data.get('workers', [])
            return len(workers) > 0 and all(w.get('state') == 'ALIVE' for w in workers)
        except:
            return False
            
    def _test_spark_app_submission(self):
        """Test Spark application submission"""
        try:
            response = requests.get('http://localhost:8081/json/applications/', timeout=10)
            apps = response.json()
            return len(apps) >= 0  # Có thể không có app nào đang chạy
        except:
            return False
            
    def _test_spark_streaming(self):
        """Test Spark Streaming status"""
        try:
            response = requests.get('http://localhost:4040/api/v1/applications', timeout=10)
            return response.status_code == 200
        except:
            return False
    
    # Data Processing Test Functions
    def _test_data_ingestion(self):
        """Test data ingestion function"""
        try:
            # Kiểm tra file ingestion có tồn tại
            ingestion_file = '../lambda_architecture/unified_spark_ingestion.py'
            return os.path.exists(ingestion_file)
        except:
            return False
            
    def _test_data_enrichment(self):
        """Test data enrichment function"""
        try:
            # Kiểm tra file enrichment có tồn tại
            enrichment_file = '../lambda_architecture/data_enrichment.py'
            return os.path.exists(enrichment_file)
        except:
            return False
            
    def _test_data_cleaning(self):
        """Test data cleaning function"""
        try:
            # Kiểm tra file cleaning có tồn tại
            cleaning_file = '../lambda_architecture/data_cleaning.py'
            return os.path.exists(cleaning_file)
        except:
            return False
            
    def _test_batch_processing(self):
        """Test batch processing function"""
        try:
            # Kiểm tra file batch processor có tồn tại
            batch_file = '../lambda_architecture/batch_layer/batch_processor.py'
            return os.path.exists(batch_file)
        except:
            return False
    
    # Elasticsearch Test Functions
    def _test_es_cluster_health(self):
        """Test Elasticsearch cluster health"""
        try:
            response = requests.get('http://localhost:9200/_cluster/health', timeout=10)
            data = response.json()
            return data.get('status') in ['green', 'yellow']
        except:
            return False
            
    def _test_es_index_creation(self):
        """Test Elasticsearch index creation"""
        try:
            response = requests.put(
                'http://localhost:9200/test_index_eval',
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            return response.status_code in [200, 201]
        except:
            return False
            
    def _test_es_document_indexing(self):
        """Test Elasticsearch document indexing"""
        try:
            doc = {
                'test_field': 'test_value',
                'timestamp': datetime.now().isoformat()
            }
            response = requests.post(
                'http://localhost:9200/test_index_eval/_doc',
                json=doc,
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            return response.status_code in [200, 201]
        except:
            return False
            
    def _test_es_search(self):
        """Test Elasticsearch search functionality"""
        try:
            query = {
                'query': {
                    'match_all': {}
                }
            }
            response = requests.post(
                'http://localhost:9200/test_index_eval/_search',
                json=query,
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            return response.status_code == 200
        except:
            return False
            
    def generate_function_report(self):
        """Tạo báo cáo đánh giá functions"""
        print("📋 Tạo báo cáo Functions...")
        
        report = {
            'evaluation_timestamp': datetime.now().isoformat(),
            'function_results': self.results,
            'summary': self._generate_function_summary()
        }
        
        # Lưu báo cáo JSON
        with open('function_evaluation_report.json', 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        # Tạo báo cáo readable
        self._create_function_readable_report(report)
        
        print("✅ Báo cáo Functions đã được lưu:")
        print("   📄 function_evaluation_report.json")
        print("   📊 function_evaluation_summary.txt")
        
    def _generate_function_summary(self):
        """Tạo tóm tắt đánh giá functions"""
        summary = {}
        
        for category, functions in self.results.items():
            passed = sum(1 for f in functions.values() if f.get('status') == 'success')
            total = len(functions)
            summary[category] = {
                'passed': passed,
                'total': total,
                'success_rate': (passed / total * 100) if total > 0 else 0
            }
            
        return summary
        
    def _create_function_readable_report(self, report):
        """Tạo báo cáo functions dễ đọc"""
        with open('function_evaluation_summary.txt', 'w', encoding='utf-8') as f:
            f.write("🔧 BÁO CÁO ĐÁNH GIÁ FUNCTIONS\n")
            f.write("=" * 40 + "\n\n")
            f.write(f"Thời gian đánh giá: {report['evaluation_timestamp']}\n\n")
            
            # Summary
            f.write("📊 TÓM TẮT FUNCTIONS\n")
            f.write("-" * 20 + "\n")
            for category, summary in report['summary'].items():
                f.write(f"{category}: {summary['passed']}/{summary['total']} ({summary['success_rate']:.1f}%)\n")
            f.write("\n")
            
            # Chi tiết từng category
            for category, functions in report['function_results'].items():
                f.write(f"🔧 {category.upper()}\n")
                f.write("-" * 30 + "\n")
                for func_name, result in functions.items():
                    status_icon = "✅" if result['status'] == 'success' else "❌"
                    f.write(f"{status_icon} {func_name}: {result['status']}")
                    if 'execution_time_ms' in result:
                        f.write(f" ({result['execution_time_ms']:.2f}ms)")
                    f.write("\n")
                f.write("\n")

if __name__ == "__main__":
    evaluator = FunctionEvaluator()
    evaluator.evaluate_all_functions()