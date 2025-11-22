import sys
import subprocess
import time
import threading

class LambdaOrchestrator:
    def __init__(self):
        self.processes = {}
    
    def start_data_ingestion(self):
        # cmd = ["python", "lambda_architecture/data_ingestion.py"]
        # self.processes['ingestion'] = subprocess.Popen(cmd)
        # print("✓ Data ingestion started")
        
        # Ensure target directory exists in HDFS
        # subprocess.run([
        #     "docker", "exec", "namenode",
        #     "hdfs", "dfs", "-mkdir", "-p", "/data/water_meter/raw"
        # ], check=True)

        # Start Kafka Connect ingestion script (runs REST API registration)
        cmd = [sys.executable, "lambda_architecture/data_ingestion.py"]
        self.processes["ingestion"] = subprocess.Popen(cmd)
        print("✓ Data ingestion (Kafka → HDFS via Kafka Connect) started")

    
    def start_batch_layer(self):
        # Schedule batch jobs every hour
        def run_batch():
            while True:
                subprocess.run([
                    "docker", "exec", "spark-master",
                    "/opt/spark/bin/spark-submit",
                    "--master", "spark://spark-master:7077",
                    "/opt/spark-apps/lambda_architecture/batch_layer/batch_processor.py"
                ])
                time.sleep(3600)  # 1 hour
        
        thread = threading.Thread(target=run_batch, daemon=True)
        thread.start()
        print("✓ Batch layer scheduled")
    
    def start_speed_layer(self):
        cmd = [
            "docker", "exec", "spark-master",
            "/opt/spark/bin/spark-submit",
            "--master", "spark://spark-master:7077",
            "/opt/spark-apps/lambda_architecture/speed_layer/stream_processor.py"
        ]
        self.processes['speed'] = subprocess.Popen(cmd)
        print("✓ Speed layer started")
    
    def upload_metadata_to_hdfs(self):
        """
        Upload metadata CSV to HDFS before processing.
        """
        # Copy file into container's /tmp
        subprocess.run(["docker", "cp", "./data/managedobject_details.csv", "namenode:/tmp/managedobject_details.csv"], check=True)
        # Upload to HDFS from inside container    
        subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", "/data/water_meter/metadata/"], check=True)
        subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs", "-put", "-f", "/tmp/managedobject_details.csv", "/data/water_meter/metadata/"], check=True)
        
        subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", "/data/water_meter/raw/"], check=True)
        print("✓ Metadata uploaded to HDFS")
    
    def start_all(self):
        print("Starting Lambda Architecture...")

        # ✅ Upload metadata first
        self.upload_metadata_to_hdfs()

        # ✅ Then start all layers
        self.start_data_ingestion()
        self.start_batch_layer()
        # self.start_speed_layer()

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
