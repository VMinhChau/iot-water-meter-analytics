wget https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar
wget https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar

docker cp delta-spark_2.12-3.2.0.jar spark-master:/opt/spark/jars/
docker cp delta-storage-3.2.0.jar     spark-master:/opt/spark/jars/

docker cp delta-spark_2.12-3.2.0.jar spark-worker-1:/opt/spark/jars/
docker cp delta-storage-3.2.0.jar     spark-worker-1:/opt/spark/jars/

docker cp delta-spark_2.12-3.2.0.jar spark-worker-2:/opt/spark/jars/
docker cp delta-storage-3.2.0.jar     spark-worker-2:/opt/spark/jars/

docker compose restart spark-master spark-worker-1 spark-worker-2