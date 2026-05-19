# Issue 04: spark_processor.py — Stream Processing Layer

## Tujuan
Membuat Apache Spark Structured Streaming script yang membaca dari Kafka, membersihkan data, dan menulis ke Google BigQuery.

## Spesifikasi

### Konfigurasi (dari `.env`)
- `KAFKA_BOOTSTRAP_SERVERS` — `kafka:29092`
- `KAFKA_TOPIC` — `idx_sector_ticks`
- `GCP_PROJECT_ID` — project ID BigQuery
- `GCP_BIGQUERY_DATASET` — dataset name
- `GCP_BIGQUERY_TABLE` — table name
- `SPARK_APP_NAME` — nama aplikasi Spark
- `SPARK_CHECKPOINT_DIR` — direktori checkpoint

### Alur Program

```
1. Load .env menggunakan python-dotenv
2. Buat SparkSession dengan:
   spark = SparkSession.builder \
     .appName(SPARK_APP_NAME) \
     .config("spark.sql.adaptive.enabled", "true") \
     .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
     .getOrCreate()
3. Definisi schema StructType:
   StructType([
     StructField("ticker", StringType()),
     StructField("sector", StringType()),
     StructField("timestamp", StringType()),
     StructField("fetch_ts", StringType()),
     StructField("open", DoubleType()),
     StructField("high", DoubleType()),
     StructField("low", DoubleType()),
     StructField("close", DoubleType()),
     StructField("volume", LongType())
   ])
4. Baca Kafka stream:
   df = spark.readStream \
     .format("kafka") \
     .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
     .option("subscribe", KAFKA_TOPIC) \
     .option("startingOffsets", "latest") \
     .option("failOnDataLoss", "false") \
     .load()
5. Parse JSON value:
   - df.selectExpr("CAST(value AS STRING)")
   - from_json() dengan schema di atas
   - Pilih kolom hasil parse (*)
6. Data Cleaning:
   - Timestamp → to_timestamp()
   - NaN di kolom numerik → None (Spark akan handle sebagai NULL)
   - Filter: hapus row yang ticker-nya null/empty
   - Drop kolom duplikat jika ada
7. Tulis ke BigQuery:
   df.writeStream \
     .format("bigquery") \
     .option("table", f"{GCP_PROJECT_ID}.{GCP_BIGQUERY_DATASET}.{GCP_BIGQUERY_TABLE}") \
     .option("checkpointLocation", SPARK_CHECKPOINT_DIR) \
     .option("writeMethod", "direct") \
     .outputMode("append") \
     .start() \
     .awaitTermination()
```

### Kredensial GCP
- Service account JSON di-mount dari `docker-compose.yml` ke `/app/gcp-service-account.json`
- Gunakan `GOOGLE_APPLICATION_CREDENTIALS` dari `.env` → `./gcp-service-account.json`

### Connector JAR yang Dibutuhkan
- `spark-sql-kafka-0-10_2.12` — Kafka source
- `spark-bigquery-with-dependencies_2.12` — BigQuery sink

Pastikan JAR tersedia di classpath Spark (via `--packages` di spark-submit atau pre-download di Dockerfile).

### Fault Tolerance
- Checkpointing ke `SPARK_CHECKPOINT_DIR`
- `failOnDataLoss=false` untuk toleransi gap data
- `startingOffsets=latest` (default), ganti ke `earliest` untuk debugging

## Acceptance Criteria
- [ ] Spark job terkoneksi ke Kafka dan membaca data
- [ ] Data berhasil ditulis ke BigQuery dengan schema yang benar
- [ ] Checkpoint berfungsi — restart tidak kehilangan progress
- [ ] NaN/null tidak menyebabkan error di BigQuery
- [ ] Tidak ada hardcoded credential
