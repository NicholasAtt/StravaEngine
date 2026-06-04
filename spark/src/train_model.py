from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
import redis
import os

spark = SparkSession.builder.appName("StravaModelTraining").getOrCreate()

df = spark.read.csv(
    "/opt/spark-input/historical_runs.csv",
    header=True,
    inferSchema=True
)

df_train, df_val = df.randomSplit([0.7, 0.3], seed=42)

df_train.cache()
df_train.count()

df_val.write.csv(
    "/opt/spark-data/dataset_validation",
    header=True,
    mode="overwrite"
)

feature_cols = [
    "cadence",
    "elevation",
    "distance",
    "temperature",
    "humidity",
    "aqi"
]

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features"
)

rf = RandomForestRegressor(
    labelCol="heart_rate",
    featuresCol="features",
    numTrees=30,      
    maxDepth=6,       
    seed=42
)

pipeline = Pipeline(stages=[assembler, rf])
model = pipeline.fit(df_train)

model.write().overwrite().save("/opt/spark-data/models/hr_fatigue_model")
print("Modello addestrato e salvato con successo!")

# Calcolo RMSE e salvataggio soglia dinamica in Redis
predictions = model.transform(df_val)
evaluator = RegressionEvaluator(labelCol="heart_rate", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print(f"RMSE sul validation set: {rmse:.4f}")

try:
    redis_host = os.getenv("REDIS_HOST", "redis")
    redis_port = int(os.getenv("REDIS_PORT", 6379))
    r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
    r.set("model:fatigue_threshold", round(rmse, 2))
    print(f"Soglia fatica salvata in Redis: {round(rmse, 2)}")
except Exception as e:
    print(f"Impossibile salvare soglia in Redis: {e}")

print("Dataset di validation salvato con successo!")

df_train.unpersist()
spark.stop()