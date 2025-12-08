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

    def start_data_ingestion(self):
        cmd = [
            "docker", "exec", "spark-master",
            "/opt/spark/bin/spark-submit",
            # "--jars", "/opt/spark/jars/delta-core_2.12-2.4.0.jar",
            "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_3.12:3.0.0",
            "--master", "spark://spark-master:7077",
            "/opt/spark-apps/lambda_architecture/unified_spark_ingestion.py"
        ]

        self.processes['ingestion'] = subprocess.Popen(cmd, stdout=sys.stdout, stderr=sys.stderr)
        print("✓ Data ingestion started")

    def start_batch_layer(self):
        """Batch Layer every hour → Delta Lake"""
        def run_batch():
            while True:
                print("Running batch processing → Delta Lake...")
                subprocess.run([
                    "docker", "exec", "spark-master",
                    "/opt/spark/bin/spark-submit",
                    "--master", "spark://spark-master:7077",
                    "--jars", "/opt/spark/jars/delta-spark_2.12-3.0.0.jar",
                    "/opt/spark-apps/lambda_architecture/batch_layer/batch_processor.py"
                ], check=True)
                print("✓ Batch to Delta Lake completed")
                time.sleep(120)  # every hour

        thread = threading.Thread(target=run_batch, daemon=True)
        thread.start()
        print("✓ Batch layer scheduled (Delta Lake, local Docker)")


    def start_speed_layer(self):
        """Start Speed Layer (Real-time streaming)"""
        cmd = [
            "docker", "exec", "spark-master",
            "/opt/spark/bin/spark-submit",
            "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.15.2",
            "--master", "spark://spark-master:7077",
            "/opt/spark-apps/lambda_architecture/speed_layer/stream_processor.py"
        ]

        self.processes['speed'] = subprocess.Popen(cmd, stdout=sys.stdout, stderr=sys.stderr)
        print("✓ Speed layer started")
        print("Speed Layer → Elasticsearch → Kibana: water-meter-realtime index")

    def setup_metadata(self):
        """Upload metadata to HDFS before starting processing"""
        subprocess.run(["docker", "cp", "./data/managedobject_details.csv", "namenode:/tmp/managedobject_details.csv"], check=True)
        # Upload to HDFS from inside container    
        subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", "/data/water_meter/metadata/"], check=True)
        subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs", "-put", "-f", "/tmp/managedobject_details.csv", "/data/water_meter/metadata/"], check=True)
        subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", "/data/water_meter/raw/"], check=True)
        print("✓ Metadata uploaded to HDFS")
    
    def upload_hdfs_to_dbfs(self):
        """Copy batch data from HDFS Docker → local orchestrator → upload DBFS"""
        LOCAL_TMP_PATH = "/tmp/water_meter_data/"
        HDFS_RAW_PATH = "/data/water_meter/raw/"

        os.makedirs(LOCAL_TMP_PATH, exist_ok=True)

        print("Downloading batch data from HDFS Docker to local...")
        subprocess.run([
            "docker", "exec", "namenode",
            "hdfs", "dfs", "-get", f"{HDFS_RAW_PATH}*", LOCAL_TMP_PATH
        ], check=True)

        print("Uploading batch data to DBFS...")
        for file_name in os.listdir(LOCAL_TMP_PATH):
            local_file = os.path.join(LOCAL_TMP_PATH, file_name)
            dbfs_path = f"dbfs:/data/water_meter/raw/{file_name}"
            subprocess.run([
                "databricks", "fs", "cp", local_file, dbfs_path, "--overwrite"
            ], check=True)

        print("✓ Upload to DBFS completed")
    
    def start_all(self):
        """Start complete Lambda Architecture"""
        print("Starting Lambda Architecture...")
        print("=" * 50)
        
        # Setup metadata first
        self.setup_metadata()
        # if not self.setup_metadata():
        #     print("Cannot proceed without metadata. Exiting.")
        #     sys.exit(1)

        # self.start_data_ingestion()
        
        # Start Speed Layer (Real-time)
        # self.start_speed_layer()
        
        # Start Batch Layer (Scheduled)
        self.start_batch_layer()
        
        print("\nBatch and Speed layers running separately!")
        print("\nReal-time Flow:")
        print("  IoT Data → Kafka → Spark Streaming → Elasticsearch → Kibana")
        print("\nBatch Flow:")
        print("  HDFS → Spark (Databricks) → Hive → Tableau")
        print("\nTo export to Tableau: python visualization/tableau_connectors.py")
        print("\nPress Ctrl+C to stop all processes")

    def stop_all(self):
        """Stop all processes"""
        for name, process in self.processes.items():
            if process.poll() is None:
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