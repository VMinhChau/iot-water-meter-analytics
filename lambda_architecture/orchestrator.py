import subprocess
import time
import threading

class LambdaOrchestrator:
    def __init__(self):
        self.processes = {}
    
    def start_data_ingestion(self):
        cmd = ["python", "lambda_architecture/data_ingestion.py"]
        self.processes['ingestion'] = subprocess.Popen(cmd)
        print("✓ Data ingestion started")
    
    def start_batch_layer(self):
        # Schedule batch jobs every hour
        def run_batch():
            while True:
                subprocess.run(["spark-submit", "lambda_architecture/batch_layer/batch_processor.py"])
                time.sleep(3600)  # 1 hour
        
        thread = threading.Thread(target=run_batch, daemon=True)
        thread.start()
        print("✓ Batch layer scheduled")
    
    def start_speed_layer(self):
        cmd = ["spark-submit", "lambda_architecture/speed_layer/stream_processor.py"]
        self.processes['speed'] = subprocess.Popen(cmd)
        print("✓ Speed layer started")
    
    def start_all(self):
        print("Starting Lambda Architecture...")
        self.start_data_ingestion()
        self.start_batch_layer()
        self.start_speed_layer()
        print("Batch and Speed layers running separately!")
        print("Speed Layer → Kibana: water-meter-speed index")
        print("Batch Layer → Tableau: Run tableau_connectors.py for CSV export")
    
    def stop_all(self):
        for name, process in self.processes.items():
            process.terminate()
            print(f"✓ {name} stopped")

if __name__ == "__main__":
    orchestrator = LambdaOrchestrator()
    try:
        orchestrator.start_all()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        orchestrator.stop_all()