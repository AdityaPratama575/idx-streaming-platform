# Issue 09: Documentation & Operational Gaps

## Tujuan
Memperbaiki dokumentasi yang outdated dan menambahkan mekanisme operasional yang hilang untuk production-readiness.

---

## Gap 1: `docs/instruction.md` — Directory Structure Outdated

### Lokasi
`docs/instruction.md` baris 49-60:
```text
idx-streaming-platform/
├── .env
├── .env.example
├── .gitignore
├── gcp-service-account.json
├── docker-compose.yml
├── producer.py
├── spark_processor.py
├── top5_saham_ihsg_by_sector_market_cap.py
└── README.md
```

### Masalah
Struktur direktori di dokumentasi tidak mencantumkan:
- `Dockerfile.producer` dan `Dockerfile.spark`
- `requirements-producer.txt` dan `requirements-spark.txt`
- `docs/` subdirectory (`agents.md`, `instruction.md`)
- `issue/` subdirectory

Developer baru yang membaca dokumen ini akan bingung melihat file yang tidak ada di tree.

### Perbaikan
Update tree struktur dengan file/direktori aktual (sinkron dengan `README.md` section "Project Structure").

---

## Gap 2: Tidak Ada Dead Letter Queue (DLQ) untuk Malformed Messages

### Lokasi
`spark_processor.py` baris 60-64 — parsing JSON:
```python
parsed_df = (
    raw_df.selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), SCHEMA).alias("parsed"))
    .select("parsed.*")
)
```

### Masalah
`from_json()` dengan schema fixed akan menghasilkan **NULL untuk semua kolom** jika JSON tidak cocok schema. Saat ini hanya ada filter `ticker.isNotNull()` (baris 68) yang men-drop row tersebut. Tapi informasi tentang message yang gagal tidak disimpan — tidak ada audit trail atau Dead Letter Queue.

Akibatnya:
- Data corruption di producer tidak terdeteksi
- Tidak bisa debug "kenapa ada data yang hilang"
- Tidak ada metrik untuk monitoring kesehatan pipeline

### Perbaikan
Tambahkan side output untuk menyimpan malformed messages ke storage terpisah:
```python
# 1. Parse JSON
parsed_df = raw_df.selectExpr("CAST(value AS STRING) as json_str") \
    .withColumn("parsed", from_json(col("json_str"), SCHEMA))

# 2. Pisahkan valid dan invalid
valid_df = parsed_df.filter(col("parsed.ticker").isNotNull()).select("parsed.*")
invalid_df = parsed_df.filter(col("parsed.ticker").isNull()).select("json_str", "topic", "partition", "offset")

# 3. Tulis stream invalid ke DLQ (misal: Kafka topic "dlq.idx_sector_ticks" atau file log)
invalid_query = invalid_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("topic", f"{KAFKA_TOPIC}_dlq") \
    .option("checkpointLocation", f"{SPARK_CHECKPOINT_DIR}_dlq") \
    .outputMode("append") \
    .start()

# 4. Valid data ke BigQuery seperti biasa
```

---

## Gap 3: Tidak Ada Schema Evolution Handling

### Lokasi
`spark_processor.py` baris 26-38 — schema hardcoded:
```python
SCHEMA = StructType([
    StructField("ticker", StringType()),
    StructField("sector", StringType()),
    # ...
])
```

### Masalah
Jika producer menambahkan field baru ke payload JSON (misal: `"change_percent"`, `"market_cap"`), semua field menjadi NULL di Spark karena schema tidak kompatibel. **Tidak ada mekanisme schema evolution atau versioning.**

Ini adalah **tight coupling** antara producer dan processor — setiap perubahan schema di producer membutuhkan deployment Spark processor.

### Perbaikan (opsi, pilih salah satu)
1. **Schema Registry** (Kafka Schema Registry) — producer register schema, Spark membaca schema dari registry. Most robust tapi butuh komponen tambahan.
2. **Schema version di header** — producer kirim `schema_version: 1` di Kafka header, Spark pilih schema parser yang sesuai.
3. **Schema-on-read dengan sampling** — Spark baca JSON sebagai MapType dulu, infer schema dari data aktual, baru parse. Lebih fleksibel tapi kurang performant.
4. **Dokumentasi kontrak schema** — Minimal, buat dokumen yang menyatakan bahwa schema di `producer.py:build_payload()` dan `spark_processor.py:SCHEMA` **harus disinkronkan manual**.

> **Rekomendasi**: Untuk fase saat ini, gunakan opsi 4 (dokumentasi kontrak) + tambahkan komentar `# SCHEMA CONTRACT: Harus identik dengan build_payload() di producer.py` di kedua file. Verifikasi bisa ditambahkan via integration test di masa depan.

---

## Gap 4: Tidak Ada Resource Limits di docker-compose.yml

### Lokasi
`docker-compose.yml` — semua service.

### Masalah
Tidak ada `deploy.resources.limits` (atau `mem_limit`, `cpus` untuk Compose v2) di service manapun. Akibatnya:
- Spark worker bisa mengonsumsi semua RAM host (OOM host)
- Kafka heap bisa tumbuh tidak terkendali
- Tidak ada jaminan QoS antar service

### Perbaikan
Tambahkan resource limits yang masuk akal:
```yaml
services:
  kafka:
    # ...
    mem_limit: 2g
    cpus: 1.0

  spark-master:
    # ...
    mem_limit: 1g
    cpus: 0.5

  spark-worker:
    # ...
    mem_limit: 2g
    cpus: 1.0

  producer:
    # ...
    mem_limit: 512m
    cpus: 0.5

  spark-processor:
    # ...
    mem_limit: 1g
    cpus: 1.0
```

Nilai-nilai di atas adalah rekomendasi untuk development local. Sesuaikan dengan resource host.

---

## Gap 5: README Verification Steps Tidak Lengkap

### Lokasi
`README.md` baris 48-53 — section "Verify".

### Masalah
Langkah verifikasi saat ini hanya menyebutkan Web UI port dan Docker exec. Tidak ada:
- Perintah untuk memverifikasi data sampai ke BigQuery (query SQL contoh)
- Cara cek log masing-masing service
- Cara graceful shutdown (`docker-compose down`)

### Perbaikan
Tambahkan ke section "Verify" di README:
```markdown
### 4. Verify

- **Spark Master UI:** http://localhost:8080
- **Spark Worker UI:** http://localhost:8081
- **Kafka topics:** `docker exec idx-kafka kafka-topics --bootstrap-server localhost:29092 --list`
- **Kafka messages count:** `docker exec idx-kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:29092 --topic idx_sector_ticks --time -1`
- **Producer logs:** `docker logs -f idx-producer`
- **Spark processor logs:** `docker logs -f idx-spark-processor`
- **BigQuery — cek data terbaru:**
  ```sql
  SELECT ticker, sector, timestamp, close, volume
  FROM `your-project.idx_stock_data.top_sector_ticks`
  WHERE timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
  ORDER BY timestamp DESC
  LIMIT 20;
  ```

### 5. Shutdown
```bash
docker-compose down          # Stop semua container
docker-compose down -v       # Stop + hapus volumes (checkpoint & Kafka data)
```
```

---

## Acceptance Criteria

- [ ] `docs/instruction.md` mencerminkan struktur direktori aktual
- [ ] Malformed JSON messages tercapture ke DLQ (Kafka topic terpisah atau file log)
- [ ] Ada komentar `SCHEMA CONTRACT` di `producer.py` dan `spark_processor.py` yang menyebutkan kedua file harus sinkron
- [ ] Resource limits ada di `docker-compose.yml` untuk semua service
- [ ] README verification section mencakup BigQuery query, cara cek log, dan shutdown procedure
- [ ] Tidak ada regresi pada pipeline saat dijalankan
