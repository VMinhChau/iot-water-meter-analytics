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
            "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
            "--master", "spark://spark-master:7077",
            "/opt/spark-apps/lambda_architecture/unified_spark_ingestion.py"
        ]

        self.processes['ingestion'] = subprocess.Popen(cmd, stdout=sys.stdout, stderr=sys.stderr)
        print("✓ Data ingestion started")

    def start_batch_layer(self):
        """Schedule batch jobs every hour"""
        def run_batch():
            while True:
                print("Running batch processing...")

                # 1️⃣ Upload batch data from HDFS Docker → DBFS
                self.upload_hdfs_to_dbfs()

                # 2️⃣ Trigger Spark batch job in Databricks
                # Bạn có thể trigger bằng Databricks Jobs API hoặc spark-submit nếu cluster tự quản
                subprocess.run([
                    "databricks", "jobs", "run-now",
                    "--job-id", os.getenv("DATABRICKS_JOB_ID")
                ], check=True)

                # Run batch processing
                subprocess.run([
                    "docker", "exec", "spark-master",
                    "/opt/spark/bin/spark-submit",
                    "--master", "spark://spark-master:7077",
                    "/opt/spark-apps/lambda_architecture/local_spark_batch.py"
                ])

                # Run analytics
                subprocess.run(["python", os.path.join(self.base_path, "hive_analytics.py")])
                print("✓ Batch processing completed")
                time.sleep(3600)  # 1 hour
        
        thread = threading.Thread(target=run_batch, daemon=True)
        thread.start()
        print("✓ Batch layer scheduled (runs every hour)")
        print("Batch Layer → HDFS → Databricks → Hive → Tableau")

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
        # self.setup_metadata()
        # if not self.setup_metadata():
        #     print("Cannot proceed without metadata. Exiting.")
        #     sys.exit(1)

        # self.start_data_ingestion()
        
        # Start Speed Layer (Real-time)
        self.start_speed_layer()
        
        # Start Batch Layer (Scheduled)
        # self.start_batch_layer()
        
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