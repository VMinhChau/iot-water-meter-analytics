# server to connect to tableau

docker exec -it spark-master bash

/opt/spark/sbin/start-thriftserver.sh \
    --master spark://spark-master:7077 \
    --conf spark.sql.catalogImplementation=hive \
    --conf spark.sql.warehouse.dir=/user/hive/warehouse \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog