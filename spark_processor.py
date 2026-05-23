import json
import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, struct, to_json, to_timestamp, when, isnan
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

# Load konfigurasi dari .env
load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCP_BIGQUERY_DATASET = os.getenv("GCP_BIGQUERY_DATASET")
GCP_BIGQUERY_TABLE = os.getenv("GCP_BIGQUERY_TABLE")
GCS_TEMP_BUCKET = os.getenv("GCS_TEMP_BUCKET")
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME")
SPARK_CHECKPOINT_DIR = os.getenv("SPARK_CHECKPOINT_DIR")

# Baca path credential dari .env (GOOGLE_APPLICATION_CREDENTIALS) agar BAN 1 compliance
# Fallback ke /app/gcp-service-account.json jika env tidak di-set
CREDENTIAL_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/app/gcp-service-account.json")
abs_cred_path = os.path.abspath(CREDENTIAL_PATH)
if os.path.exists(abs_cred_path):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = abs_cred_path

# Schema JSON yang dikirim oleh producer (harus sama persis di kedua sisi pipeline)
SCHEMA = StructType(
    [
        StructField("ticker", StringType()),
        StructField("sector", StringType()),
        StructField("timestamp", StringType()),
        StructField("fetch_ts", StringType()),
        StructField("open", DoubleType()),
        StructField("high", DoubleType()),
        StructField("low", DoubleType()),
        StructField("close", DoubleType()),
        StructField("volume", LongType()),
    ]
)

spark = (
    SparkSession.builder.appName(SPARK_APP_NAME)
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.jars",
        "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.4.4.jar,"
        "/opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.4.4.jar,"
        "/opt/spark/jars/kafka-clients-3.4.0.jar,"
        "/opt/spark/jars/commons-pool2-2.11.1.jar,"
        "/opt/spark/jars/gcs-connector-hadoop3-2.2.11-shaded.jar,"
        "/opt/spark/jars/spark-bigquery-with-dependencies_2.12-0.34.0.jar")
    .config("spark.executorEnv.GOOGLE_APPLICATION_CREDENTIALS", abs_cred_path)
    .config("spark.hadoop.fs.gs.auth.service.account.enable", "true")
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", abs_cred_path)
    .config("spark.ui.prometheus.enabled", "true")
    .config("spark.metrics.conf.*.sink.prometheus.class", "org.apache.spark.metrics.sink.PrometheusServlet")
    .config("spark.metrics.conf.*.sink.prometheus.path", "/metrics/prometheus")
    .config("spark.sql.streaming.metricsEnabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("INFO")

print("[BOOT] Starting Kafka stream reader...")

# Baca stream dari Kafka topic
raw_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# Parse value dari JSON bytes ke string, konversi kolom value ke JSON melalui from_json
# Mode PERMISSIVE: field tidak dikenal → NULL (forward compatibility)
raw_with_str = raw_df.selectExpr(
    "CAST(value AS STRING) as json_str",
    "headers",
    "topic",
    "partition",
    "offset",
)
parsed_df = raw_with_str.withColumn(
    "parsed",
    from_json(col("json_str"), SCHEMA, {"mode": "PERMISSIVE"}),
)

# Pisahkan data valid (parsed != null) vs invalid (malformed JSON)
valid_raw = parsed_df.filter(col("parsed").isNotNull())
invalid_df = parsed_df.filter(col("parsed").isNull()).select(
    to_json(struct("json_str", "topic", "partition", "offset")).alias("value")
)

# Flatten struct parsed ke kolom top-level agar bisa diakses langsung
flattened = valid_raw.select("parsed.*")

# Data cleaning:
# 1. Hapus row dengan ticker null/empty
cleaned_df = flattened.filter(col("ticker").isNotNull() & (col("ticker") != ""))

# 2. NaN di kolom DoubleType → NULL (BigQuery tidak menerima NaN)
double_cols = ["open", "high", "low", "close"]
for c in double_cols:
    cleaned_df = cleaned_df.withColumn(c, when(isnan(col(c)), None).otherwise(col(c)))

# 3. Parse ISO 8601 timestamp (format: 2026-05-06T15:47:00+07:00)
cleaned_df = cleaned_df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ssXXX"))
cleaned_df = cleaned_df.withColumn("fetch_ts", to_timestamp(col("fetch_ts"), "yyyy-MM-dd'T'HH:mm:ssXXX"))

# 4. Dedup dengan watermark agar state tidak tumbuh unbounded (OOM safety)
cleaned_df = cleaned_df.withWatermark("timestamp", "30 minutes") \
    .dropDuplicates(["ticker", "timestamp"])

print("[BOOT] Starting DLQ stream...")

# Tulis stream invalid ke DLQ (Dead Letter Queue) — Kafka topic terpisah untuk debugging
dlq_query = (
    invalid_df.writeStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("topic", KAFKA_TOPIC + "_dlq")
    .option("checkpointLocation", SPARK_CHECKPOINT_DIR + "_dlq")
    .outputMode("append")
    .start()
)

print("[BOOT] Starting BigQuery stream...")

# Tulis stream valid ke BigQuery
bq_query = (
    cleaned_df.writeStream.format("bigquery")
    .option("table", f"{GCP_PROJECT_ID}.{GCP_BIGQUERY_DATASET}.{GCP_BIGQUERY_TABLE}")
    .option("temporaryGcsBucket", GCS_TEMP_BUCKET)
    .option("checkpointLocation", SPARK_CHECKPOINT_DIR)
    .option("writeMethod", "direct")
    .outputMode("append")
    .start()
)

# Tunggu kedua stream berjalan — jika salah satu berhenti, pipeline berhenti
spark.streams.awaitAnyTermination()
