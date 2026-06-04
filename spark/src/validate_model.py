from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import col, round as spark_round

spark = SparkSession.builder.appName("StravaModelValidation").getOrCreate()

df_val = spark.read.csv(
    "/opt/spark-data/dataset_validation",
    header=True,
    inferSchema=True
)

model = PipelineModel.load("/opt/spark-data/models/hr_fatigue_model")

rf_model = model.stages[-1]
importances = rf_model.featureImportances.toArray()

feature_cols = [
    "cadence",
    "elevation",
    "distance",
    "temperature",
    "humidity",
    "aqi"
]

print("Stampa dell'importanza delle features")
for feat, imp in zip(feature_cols, importances):
    print(f"{feat}: {imp:.4f}")

predictions = model.transform(df_val)

evaluator_rmse = RegressionEvaluator(
    labelCol="heart_rate",
    predictionCol="prediction",
    metricName="rmse"
)

rmse = evaluator_rmse.evaluate(predictions)
print(f"RMSE sul validation set: {rmse:.4f}")

evaluator_mae = RegressionEvaluator(
    labelCol="heart_rate",
    predictionCol="prediction",
    metricName="mae"
)

mae = evaluator_mae.evaluate(predictions)
print(f"MAE sul validation set: {mae:.4f}")

evaluator_r2 = RegressionEvaluator(
    labelCol="heart_rate",
    predictionCol="prediction",
    metricName="r2"
)

r2 = evaluator_r2.evaluate(predictions)
print(f"R2 sul validation set: {r2:.4f}")

results = predictions.select(
    "cadence",
    "elevation",
    "distance",
    "temperature",
    "humidity",
    "aqi",
    "heart_rate",
    spark_round(col("prediction"), 2).alias("predicted_heart_rate")
)

results.show(20, truncate=False)

print("Validazione completata correttamente!")

spark.stop()