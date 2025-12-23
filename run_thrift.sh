docker exec -d spark-thrift /opt/spark/sbin/start-thriftserver.sh \
  --master spark://spark-master:7077 \
  --packages io.delta:delta-spark_2.12:3.1.0 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.catalogImplementation=hive \
  --hiveconf hive.metastore.uris=thrift://hive-metastore:9083 \
  --hiveconf hive.metastore.warehouse.dir=/opt/spark/work-dir/spark-warehouse \
  --hiveconf hive.server2.thrift.port=10000