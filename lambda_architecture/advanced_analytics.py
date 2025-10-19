from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml import Pipeline
import logging
import os

logger = logging.getLogger(__name__)

class AdvancedAnalytics:
    def __init__(self, spark, model_path="/tmp/models"):
        self.spark = spark
        self.model_path = model_path
        os.makedirs(model_path, exist_ok=True)
    
    def detect_usage_patterns(self, df):
        """Detect consumption patterns using ML"""
        
        # Hourly consumption patterns
        hourly_patterns = df.filter(col("measurement_type") == "Pulse1") \
            .withColumn("hour", hour(col("timestamp"))) \
            .groupBy("meter_id", "hour") \
            .agg(avg("value").alias("avg_hourly_flow"))
        
        # Pivot to create feature matrix
        pattern_matrix = hourly_patterns.groupBy("meter_id") \
            .pivot("hour") \
            .agg(first("avg_hourly_flow"))
        
        # Fill nulls and create feature vector
        feature_cols = [f"`{i}`" for i in range(24)]
        filled_matrix = pattern_matrix.fillna(0, subset=feature_cols)
        
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        feature_df = assembler.transform(filled_matrix)
        
        # Feature scaling for better clustering
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
        
        # K-means clustering pipeline
        kmeans = KMeans(k=4, seed=42, featuresCol="scaled_features")
        pipeline = Pipeline(stages=[scaler, kmeans])
        
        # Train model
        model = pipeline.fit(feature_df)
        clustered = model.transform(feature_df)
        
        # Evaluate clustering quality
        evaluator = ClusteringEvaluator(featuresCol="scaled_features")
        silhouette = evaluator.evaluate(clustered)
        logger.info(f"Clustering silhouette score: {silhouette:.3f}")
        
        # Save model
        model_save_path = f"{self.model_path}/usage_patterns_model"
        model.write().overwrite().save(model_save_path)
        logger.info(f"Model saved to {model_save_path}")
        
        return clustered.select("meter_id", "prediction", "scaled_features").withColumnRenamed("prediction", "usage_pattern")
    
    def calculate_efficiency_metrics(self, df):
        """Calculate water usage efficiency metrics"""
        
        return df.filter(col("measurement_type") == "Pulse1_Total") \
            .groupBy("meter_id", "usage_type", "suburb") \
            .agg(
                avg("total_value").alias("avg_daily_usage"),
                stddev("total_value").alias("usage_variability"),
                (max("total_value") / avg("total_value")).alias("peak_to_avg_ratio")
            ).withColumn("efficiency_score",
                when(col("usage_variability") < 50, "EFFICIENT")
                .when(col("usage_variability") < 100, "MODERATE")
                .otherwise("INEFFICIENT")
            )
    
    def load_usage_pattern_model(self):
        """Load pre-trained usage pattern model"""
        model_load_path = f"{self.model_path}/usage_patterns_model"
        try:
            from pyspark.ml import PipelineModel
            model = PipelineModel.load(model_load_path)
            logger.info(f"Model loaded from {model_load_path}")
            return model
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            return None
    
    def detect_seasonal_patterns(self, df):
        """Detect seasonal consumption patterns"""
        return df.filter(col("measurement_type") == "Pulse1_Total") \
            .withColumn("month", month(col("timestamp"))) \
            .withColumn("day_of_week", dayofweek(col("timestamp"))) \
            .groupBy("meter_id", "month", "day_of_week", "suburb") \
            .agg(
                avg("value").alias("avg_consumption"),
                stddev("value").alias("consumption_std")
            ).withColumn("seasonal_pattern",
                when(col("month").isin([12, 1, 2]), "SUMMER")
                .when(col("month").isin([3, 4, 5]), "AUTUMN")
                .when(col("month").isin([6, 7, 8]), "WINTER")
                .otherwise("SPRING")
            )