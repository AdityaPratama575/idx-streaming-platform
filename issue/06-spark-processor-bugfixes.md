# Issue 06: spark_processor.py — Critical Bug Fixes & Fault Tolerance

## Tujuan
Memperbaiki 5 critical bugs pada `spark_processor.py` dan `docker-compose.yml` yang dapat menyebabkan:
- Kegagalan pipeline di production
- Kehilangan data / checkpoint
- Out-of-memory (OOM) pada streaming state
- Pelanggaran Hard Ban pada `docs/agents.md`

---

## Bug 1: Hardcoded Credential Path (BAN 1 Violation)

### Lokasi
`spark_processor.py` baris 21:
```python
CREDENTIAL_PATH = "/app/gcp-service-account.json"
```

### Masalah
Path credential di-hardcode ke `/app/gcp-service-account.json`, padahal `.env` sudah menyediakan variable `GOOGLE_APPLICATION_CREDENTIALS` (di-load via `load_dotenv()` tapi tidak pernah digunakan). Ini melanggar **BAN 1** di `docs/agents.md`:
> DO NOT hardcode the Google Service Account JSON path or credentials inside any .py file. Use the variable `GOOGLE_APPLICATION_CREDENTIALS`.

### Perbaikan
Baca path credential dari environment variable `GOOGLE_APPLICATION_CREDENTIALS`, dengan fallback ke path default:
```python
CREDENTIAL_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/app/gcp-service-account.json")
```

---

## Bug 2: Missing `spark_checkpoints` Volume Mount

### Lokasi
`docker-compose.yml` baris 120-142 — service `spark-processor`.

### Masalah
Volume `spark_checkpoints` sudah didefinisikan di top-level `volumes:` tapi **tidak di-mount** di service `spark-processor`. Akibatnya, `SPARK_CHECKPOINT_DIR=/tmp/spark-checkpoints/idx_stock` disimpan di ephemeral container filesystem dan **hilang setiap container restart**. Ini menghancurkan exactly-once semantics dan fault tolerance yang dijanjikan arsitektur.

Saat ini service `spark-processor` hanya memiliki:
```yaml
volumes:
  - ./gcp-service-account.json:/app/gcp-service-account.json:ro
```

### Perbaikan
Tambahkan mount named volume `spark_checkpoints`:
```yaml
volumes:
  - ./gcp-service-account.json:/app/gcp-service-account.json:ro
  - spark_checkpoints:/tmp/spark-checkpoints
```

---

## Bug 3: `dropDuplicates()` Tanpa Watermark — Unbounded State

### Lokasi
`spark_processor.py` baris 76:
```python
cleaned_df = cleaned_df.dropDuplicates()
```

### Masalah
Dalam Spark Structured Streaming, `dropDuplicates()` **tanpa watermark** akan menyimpan **seluruh history** record yang pernah dilihat ke state store. Karena stream berjalan terus-menerus (24/7), state store akan tumbuh **unbounded** dan menyebabkan **Out of Memory (OOM)**.

### Perbaikan
Tambahkan watermark pada kolom timestamp sebelum `dropDuplicates()`, atau gunakan `dropDuplicates(["ticker", "timestamp"])` dengan watermark:
```python
cleaned_df = cleaned_df \
    .withWatermark("timestamp", "30 minutes") \
    .dropDuplicates(["ticker", "timestamp"])
```

> **Catatan**: `dropDuplicates()` tanpa argumen (dedup semua kolom) di streaming hampir tidak pernah berguna karena setiap baris data OHLCV unik. Jika tujuannya mencegah duplikat window yang sama, gunakan watermark + dedup pada kolom spesifik (`ticker` + `timestamp`).

---

## Bug 4: `isnan()` pada LongType Column (`volume`)

### Lokasi
`spark_processor.py` baris 71-73:
```python
numeric_cols = ["open", "high", "low", "close", "volume"]
for c in numeric_cols:
    cleaned_df = cleaned_df.withColumn(c, when(isnan(col(c)), None).otherwise(col(c)))
```

### Masalah
`isnan()` adalah fungsi yang **hanya berlaku untuk `DoubleType` dan `FloatType`**. Kolom `volume` memiliki tipe `LongType` (lihat schema baris 36). `isnan()` pada `LongType` tidak menghasilkan error, tapi juga **tidak berfungsi** (selalu return `False`). Ini kode **no-op** yang misleading — NaN pada `LongType` secara konsep tidak mungkin terjadi, tetapi developer mungkin mengira kode ini melakukan sesuatu.

### Perbaikan
Pisahkan `volume` dari `numeric_cols` yang perlu NaN-check, atau ganti loop menjadi hanya untuk kolom `DoubleType`:
```python
double_cols = ["open", "high", "low", "close"]
for c in double_cols:
    cleaned_df = cleaned_df.withColumn(c, when(isnan(col(c)), None).otherwise(col(c)))
```

---

## Bug 5: `cast("timestamp")` Gagal untuk ISO 8601 dengan Timezone Offset

### Lokasi
`spark_processor.py` baris 79-80:
```python
cleaned_df = cleaned_df.withColumn("timestamp", col("timestamp").cast("timestamp"))
cleaned_df = cleaned_df.withColumn("fetch_ts", col("fetch_ts").cast("timestamp"))
```

### Masalah
Producer mengirim timestamp dalam format ISO 8601 dengan timezone offset, contoh:
```
"2026-05-05T10:30:00+07:00"
```

Spark `TimestampType` secara default hanya menerima format `yyyy-MM-dd HH:mm:ss` (tanpa timezone offset). `cast("timestamp")` bisa **menghasilkan NULL** untuk nilai dengan timezone, menyebabkan seluruh data timestamp hilang.

### Perbaikan
Gunakan `to_timestamp()` dengan format string eksplisit, atau parse ISO 8601 dengan benar:
```python
from pyspark.sql.functions import to_timestamp

cleaned_df = cleaned_df.withColumn(
    "timestamp", 
    to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ssXXX")  # atau format lain sesuai output producer
)
cleaned_df = cleaned_df.withColumn(
    "fetch_ts", 
    to_timestamp(col("fetch_ts"), "yyyy-MM-dd'T'HH:mm:ssXXX")
)
```

> **Catatan tambahan**: Periksa dulu format timestamp aktual yang dikirim producer (`datetime.now(timezone.utc).isoformat()` menghasilkan `2026-05-05T10:30:00.123456+00:00` dengan microseconds). Sesuaikan format string di `to_timestamp()` dengan output aktual.

---

## Acceptance Criteria

- [ ] `GOOGLE_APPLICATION_CREDENTIALS` dari `.env` digunakan (tidak hardcode path)
- [ ] Volume `spark_checkpoints` ter-mount di service `spark-processor` dan checkpoint bertahan setelah `docker-compose restart`
- [ ] `dropDuplicates()` memiliki watermark agar state tidak unbounded
- [ ] `isnan()` tidak dipanggil pada kolom `LongType` (`volume`)
- [ ] Timestamp ISO 8601 di-parse dengan benar, tidak menghasilkan NULL
- [ ] Tidak ada regresi pada flow: Kafka → parse JSON → clean → BigQuery
