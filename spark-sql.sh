docker exec -it spark-master \
/opt/spark/bin/spark-sql \
--packages io.delta:delta-spark_2.12:3.1.0 \
--conf spark.jars.ivy=/tmp/.ivy2 \
--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
--conf spark.sql.catalogImplementation=in-memory