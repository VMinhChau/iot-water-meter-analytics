from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

class BatchToElasticsearch:
    def __init__(self):
        self.es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
        self.spark = SparkSession.builder.appName("BatchToES").getOrCreate()
    
    def create_batch_index(self):
        mapping = {
            "mappings": {
                "properties": {
                    "meter_id": {"type": "keyword"},
                    "date": {"type": "date"},
                    "total_consumption": {"type": "float"},
                    "avg_pressure": {"type": "float"},
                    "reading_count": {"type": "integer"},
                    "data_source": {"type": "keyword"}
                }
            }
        }
        
        try:
            self.es.indices.create(index="water-meter-batch", body=mapping)
        except:
            pass
    
    def write_batch_data(self, df):
        # Add data source identifier
        df_with_source = df.withColumn("data_source", lit("batch_layer"))
        
        df_with_source.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.resource", "water-meter-batch") \
            .option("es.nodes", "localhost") \
            .option("es.port", "9200") \
            .mode("overwrite") \
            .save()

if __name__ == "__main__":
    writer = BatchToElasticsearch()
    writer.create_batch_index()