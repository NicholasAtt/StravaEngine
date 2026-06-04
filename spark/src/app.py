import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (avg, col, concat, count, date_format, from_json, lit, max as spark_max, stddev, to_timestamp, when, window)
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType
from pyspark.ml import PipelineModel
import redis
from redis.exceptions import RedisError

ELASTICSEARCH_RESOURCE = "strava_metrics"

REQUIRED_LIVE_FIELDS = ["heart_rate", "latitude", "longitude", "timestamp"]

LIVE_DEFAULTS = {
    "cadence": 0,
    "elevation": 0.0,
    "distance": 0.0,
}

DEFAULT_ENV = {
    "temperature": 20.0,
    "humidity": 50.0,
    "aqi": 50,
}

DEFAULT_FATIGUE_THRESHOLD = 15.0

def update_env_cache(partition, topic_type):
    """Aggiorna la cache ambiente usata dai microbatch live."""
    try:
        r = redis.Redis(host="redis", port=6379, decode_responses=True)
        pipe = r.pipeline()
        for row in partition:
            if topic_type == "weather":
                pipe.hset("env:current", mapping={"temperature": row.temperature, "humidity": row.humidity})
            else:
                pipe.hset("env:current", "aqi", getattr(row, "aqi", 50))
        pipe.execute()
    except RedisError as exc:
        print(f" Redis env cache skipped: {exc}")


def read_live_context():
    try:
        r = redis.Redis(host="redis", port=6379, decode_responses=True)
        env = r.hgetall("env:current")
        threshold = float(r.get("model:fatigue_threshold") or DEFAULT_FATIGUE_THRESHOLD)
        return {
            "temperature": float(env.get("temperature", DEFAULT_ENV["temperature"])),
            "humidity": float(env.get("humidity", DEFAULT_ENV["humidity"])),
            "aqi": int(env.get("aqi", DEFAULT_ENV["aqi"])),
            "fatigue_threshold": threshold,
        }
    except Exception as exc:
        print(f"Redis context unavailable, using defaults: {exc}")
        return {
            **DEFAULT_ENV,
            "fatigue_threshold": DEFAULT_FATIGUE_THRESHOLD,
        }


def write_to_elasticsearch(batch_df, mapping_id=None):
    if batch_df.isEmpty():
        return

    writer = batch_df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "elasticsearch") \
        .option("es.port", "9200") \
        .option("es.nodes.wan.only", "true") \
        .option("es.resource", ELASTICSEARCH_RESOURCE) \
        .option("es.index.auto.create", "true")

    if mapping_id:
        writer = writer.option("es.mapping.id", mapping_id)

    writer.mode("append").save()


def write_live_to_redis(partition):
    try:
        r = redis.Redis(host="redis", port=6379, decode_responses=True)
        pipe = r.pipeline()
        for row in partition:
            # Tracciamento geografico
            pipe.xadd("race:live_tracking", {
                "latitude": float(row.latitude),
                "longitude": float(row.longitude),
                "dist": float(row.distance),
                "timestamp": str(row.timestamp)
            }, maxlen=1000)

            # Monitoraggio predittivo ML e biometria
            pipe.xadd("race:live_metrics", {
                "hr": int(row.heart_rate),
                "cad": int(row.cadence),
                "expected_hr": float(row.expected_hr),
                "fatigue_alert": str(row.fatigue_alert).lower(),
                "fatigue_delta": float(row.fatigue_delta),
                "fatigue_threshold": float(row.fatigue_threshold),
                "timestamp": str(row.timestamp)
            }, maxlen=1000)
            
        pipe.execute()
    except RedisError as exc:
        print(f" Redis live streams skipped: {exc}")


def write_agg_to_redis(partition):
    try:
        r = redis.Redis(host="redis", port=6379, decode_responses=True)
        pipe = r.pipeline()
        for row in partition:
            pipe.xadd("race:cardiac_drift_stream", {
                "efficiency_factor": float(row.efficiency_factor),
            }, maxlen=100)
        pipe.execute()
    except RedisError as exc:
        print(f" Redis aggregate stream skipped: {exc}")

def read_kafka(topic):
    return spark.readStream.format("kafka").option("kafka.bootstrap.servers", "broker:9092") \
        .option("subscribe", topic).option("startingOffsets", "latest").option("failOnDataLoss", "false").load()


def build_live_result_df(batch_df):
    context = read_live_context()
    enriched_df = batch_df \
        .withColumn("temperature", lit(context["temperature"])) \
        .withColumn("humidity", lit(context["humidity"])) \
        .withColumn("aqi", lit(context["aqi"]))

    if model:
        predictions = model.transform(enriched_df)
    else:
        predictions = enriched_df.withColumn("prediction", col("heart_rate").cast("double"))

    fatigue_alert_expr = col("heart_rate") - col("prediction").cast("double") > lit(context["fatigue_threshold"])

    return predictions \
        .withColumn("expected_hr", col("prediction").cast("double")) \
        .withColumn("fatigue_delta", col("heart_rate") - col("expected_hr")) \
        .withColumn("fatigue_margin", col("fatigue_delta") - lit(context["fatigue_threshold"])) \
        .withColumn("fatigue_alert", fatigue_alert_expr) \
        .withColumn("fatigue_alert_value", when(col("fatigue_alert"), 1).otherwise(0)) \
        .withColumn("fatigue_threshold", lit(context["fatigue_threshold"])) \
        .drop("features", "prediction", "ts")

def process_live_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    clean_df = batch_df.dropna(subset=REQUIRED_LIVE_FIELDS).fillna(LIVE_DEFAULTS)
    if clean_df.isEmpty():
        return

    result_df = build_live_result_df(clean_df).persist()
    try:
        write_to_elasticsearch(result_df)
        result_df.foreachPartition(write_live_to_redis)
    finally:
        result_df.unpersist()

spark = SparkSession.builder.appName("StravaEngine_SingleRunner").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

strava_schema = StructType([
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True), 
    StructField("elevation", DoubleType(), True),
    StructField("timestamp", StringType(), True), 
    StructField("session_id", StringType(), True),
    StructField("ingestion_timestamp", StringType(), True),
    StructField("heart_rate", IntegerType(), True), 
    StructField("cadence", IntegerType(), True), 
    StructField("distance", DoubleType(), True)
])

weather_schema = StructType([
    StructField("temperature", DoubleType(), True), 
    StructField("humidity", DoubleType(), True)
])

air_schema = StructType([
    StructField("aqi", IntegerType(), True)
])


strava_df = read_kafka("running-live-data").select(from_json(col("value").cast("string"), strava_schema).alias("d")).select("d.*")
weather_df = read_kafka("weatherdata").select(from_json(col("value").cast("string"), weather_schema).alias("d")).select("d.*")
air_df = read_kafka("airquality").select(from_json(col("value").cast("string"), air_schema).alias("d")).select("d.*")

model_path = "/opt/spark-data/models/hr_fatigue_model"

if os.path.exists(model_path):
    try:
        model = PipelineModel.load(model_path)
        print("Modello di ML caricato con successo da disco.")
    except Exception as e:
        print(f"ERRORE CRITICO: Il modello esiste in {model_path} ma non può essere caricato: {e}")
        model = None
else:
    print(f"Modello non trovato in {model_path}. Procedo in modalità fallback.")
    model = None

strava_watermarked = strava_df.withColumn("ts", to_timestamp(col("timestamp"))).withWatermark("ts", "30 seconds")
aggregated_df = strava_watermarked.groupBy(window(col("ts"), "1 minute", "30 seconds")) \
    .agg(avg("heart_rate").alias("avg_hr"), avg("cadence").alias("avg_cadence"), stddev("heart_rate").alias("hr_stddev")) \
    .withColumn("efficiency_factor", when(col("avg_hr") == 0, 0.0).otherwise(col("avg_cadence") / col("avg_hr")))

session_summary_df = strava_watermarked.dropna(subset=["session_id"]).groupBy("session_id") \
    .agg(
        avg("heart_rate").alias("avg_hr"),
        avg("cadence").alias("avg_cadence"),
        count("*").alias("points_count"),
        spark_max("ts").alias("timestamp")
    ) \
    .withColumn("timestamp", date_format(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")) \
    .withColumn("event_type", lit("session_summary")) \
    .withColumn("session_status", lit("running")) \
    .withColumn("document_id", concat(lit("session_summary_"), col("session_id")))

query_weather = weather_df.writeStream.foreachBatch(lambda df, _: df.foreachPartition(lambda p: update_env_cache(p, "weather"))).option("checkpointLocation", "/tmp/spark-checkpoints/weather").start()
query_air = air_df.writeStream.foreachBatch(lambda df, _: df.foreachPartition(lambda p: update_env_cache(p, "air"))).option("checkpointLocation", "/tmp/spark-checkpoints/air").start()

query_live = strava_df.writeStream.foreachBatch(process_live_batch) \
    .trigger(processingTime="1 second").option("checkpointLocation", "/tmp/spark-checkpoints/live").start()

query_agg = aggregated_df.writeStream.foreachBatch(lambda df, _: df.foreachPartition(write_agg_to_redis)) \
    .outputMode("update").trigger(processingTime="1 second").option("checkpointLocation", "/tmp/spark-checkpoints/agg").start()

query_summary = session_summary_df.writeStream.foreachBatch(lambda df, _: write_to_elasticsearch(df, "document_id")) \
    .outputMode("update").trigger(processingTime="1 second").option("checkpointLocation", "/tmp/spark-checkpoints/session_summary").start()

spark.streams.awaitAnyTermination()
