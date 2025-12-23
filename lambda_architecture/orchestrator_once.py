import subprocess
import time
import threading
import os
import sys

class LambdaOrchestrator:
    def __init__(self):
        self.processes = {}
        self.base_path = os.path.dirname(os.path.abspath(__file__))
        self.project_root = sys.executable

    def start_batch_layer_once(self):
        """Run Batch Layer once → Delta Lake"""
        print("Running batch processing once → Delta Lake...")
        subprocess.run([
            "docker", "exec", "spark-master",
            "/opt/spark/bin/spark-submit",
            "--master", "spark://spark-master:7077",
            "--packages", "io.delta:delta-spark_2.12:3.1.0",
            "--conf", "spark.jars.ivy=/tmp/.ivy2",
            "--executor-memory", "512m",
            "/opt/spark-apps/lambda_architecture/batch_layer/batch_processor.py"
        ], check=True)
        print("✓ Batch to Delta Lake completed (one-time run)")

    def setup_metadata(self):
        """Upload metadata to HDFS before starting processing"""
        # Wait for HDFS Namenode to be ready
        for _ in range(20):
            result = subprocess.run(
                ["docker", "exec", "namenode", "hdfs", "dfs", "-ls", "/"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            if result.returncode == 0:
                break
            time.sleep(2)
        subprocess.run(["docker", "cp", "./data/managedobject_details.csv", "namenode:/tmp/managedobject_details.csv"], check=True)
        # Upload to HDFS from inside container    
        subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", "/data/water_meter/metadata/"], check=True)
        subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs", "-put", "-f", "/tmp/managedobject_details.csv", "/data/water_meter/metadata/"], check=True)
        subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", "/data/water_meter/raw/"], check=True)
        subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs", "-chmod", "-R", "777", "/data/water_meter"], check=True)
        print("✓ Metadata uploaded to HDFS")
    
    def run_once(self):
        """Run minimal orchestrator for demo"""
        print("Starting one-time batch processing demo...")
        self.setup_metadata()
        self.start_batch_layer_once()
        print("All tables processed. You can now connect Tableau.")

if __name__ == "__main__":
    orchestrator = LambdaOrchestrator()
    orchestrator.run_once()