# Issue 02: Dockerfiles & Dependencies

## Tujuan
Membuat Dockerfile kustom dan file requirements untuk producer dan spark-processor.

## Spesifikasi

### File yang Harus Dibuat

| File | Deskripsi |
|---|---|
| `Dockerfile.producer` | Docker image untuk Python producer |
| `Dockerfile.spark` | Docker image untuk Spark processor |
| `requirements-producer.txt` | Python dependencies producer |
| `requirements-spark.txt` | Python dependencies spark processor |

---

### Dockerfile.producer

Gunakan base image `python:3.9-slim`.

Steps:
1. Set workdir `/app`
2. Copy `requirements-producer.txt`
3. `pip install -r requirements-producer.txt`
4. Copy `producer.py` dan `top5_saham_ihsg_by_sector_market_cap.py`
5. Set `ENV PYTHONUNBUFFERED=1`
6. Set `CMD ["python", "producer.py"]`

---

### Dockerfile.spark

Gunakan base image `bitnami/spark:3.5` (sudah include Spark + Python).

Steps:
1. Set workdir `/app`
2. Copy `requirements-spark.txt`
3. `pip install -r requirements-spark.txt`
4. Copy `spark_processor.py`
5. Set `ENV PYTHONUNBUFFERED=1`

CMD diatur via `docker-compose.yml` (command: spark-submit ...) BUKAN di Dockerfile, karena ada argumen dinamis.

---

### requirements-producer.txt

Dependencies:
```
python-dotenv>=1.0.0
yfinance>=0.2.30
kafka-python>=2.0.2
```
(Jika pakai confluent-kafka, ganti ke `confluent-kafka>=2.3.0`)

---

### requirements-spark.txt

Dependencies:
```
python-dotenv>=1.0.0
```
Spark + Kafka + BigQuery connectors sudah bundled di base image Bitnami. Jika perlu connector tambahan, download JAR via Dockerfile:
```
spark-sql-kafka-0-10_2.12:3.5.0
spark-bigquery-with-dependencies_2.12:0.34.0
```

## Acceptance Criteria
- [ ] Kedua Dockerfile berhasil build tanpa error
- [ ] Ukuran image tetap kecil (slim base)
- [ ] Tidak ada hardcoded credential di Dockerfile
