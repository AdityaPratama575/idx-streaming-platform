# Runbook — IDX-Stream Pipeline Operational Guide

## 1. Setup Awal

### 1.1 Clone & Konfigurasi

```bash
git clone git@github.com:AdityaPratama575/idx-streaming-platform.git
cd idx-streaming-platform

# Copy template env
cp .env.example .env
```

### 1.2 Isi .env

```bash
nano .env
```

Isi minimal:
```
GCP_PROJECT_ID="idx-analytics-platform"
GOOGLE_APPLICATION_CREDENTIALS="./gcp-service-account.json"
GCP_BIGQUERY_DATASET="idx_stock_data"
GCP_BIGQUERY_TABLE="top_sector_ticks"
GCS_TEMP_BUCKET="idx-analytics-platform-temp-staging"
```

### 1.3 Service Account GCP

1. Buka [console.cloud.google.com](https://console.cloud.google.com)
2. **IAM & Admin → Service Accounts**
3. Klik service account → **KEYS → ADD KEY → Create new key → JSON**
4. Download, rename ke `gcp-service-account.json`, taruh di root project

### 1.4 Setup BigQuery

Buat dataset & table di GCP Console:
```sql
-- Dataset: idx_stock_data (location: asia-southeast2)
-- Table: top_sector_ticks

-- Atau jalankan via Python:
python3 scripts/setup_bigquery.py
```

### 1.5 Pastikan Docker Desktop Running

```bash
docker --version
docker compose version
```

> **Catatan WSL2:** Jika error "command not found", restart Docker Desktop dari Windows. Di WSL, pastikan integrasi WSL diaktifkan: Docker Desktop → Settings → Resources → WSL Integration → centang distro Anda.

---

## 2. Menjalankan Pipeline

### 2.1 Start Semua Service

```bash
cd idx-streaming-platform
docker compose up --build -d
```

| Flag | Keterangan |
|---|---|
| `--build` | Build ulang image (wajib setelah edit code) |
| `-d` | Detached mode (latar belakang) |
| Tanpa `-d` | Foreground (lihat log real-time) |

### 2.2 Cek Status Container

```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```

Output yang diharapkan:
```
NAMES                     STATUS
idx-spark-processor       Up X minutes
idx-producer              Up X minutes
idx-spark-worker          Up X minutes
idx-spark-master          Up X minutes (healthy)
idx-kafka                 Up X minutes (healthy)
```

### 2.3 Lihat Log

```bash
# Producer (lihat ticker yang di-fetch)
docker logs idx-producer -f --tail 20

# Spark processor (lihat streaming progress)
docker logs idx-spark-processor -f --tail 20

# Kafka broker
docker logs idx-kafka -f --tail 10
```

### 2.4 Verifikasi Pipeline

```bash
# 1. Cek Kafka topic
docker exec idx-kafka kafka-topics --bootstrap-server localhost:29092 --list

# 2. Cek offset Kafka (jumlah message)
docker exec idx-kafka kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server kafka:29092 \
  --topic idx_sector_ticks --time -1

# 3. Cek Spark Master UI
curl http://localhost:8080

# 4. Cek BigQuery
python3 -c "
from google.cloud import bigquery; import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'gcp-service-account.json'
client = bigquery.Client(project='idx-analytics-platform')
for row in client.query('SELECT COUNT(*) as cnt FROM \`idx-analytics-platform.idx_stock_data.top_sector_ticks\`'):
    print(f'Rows: {row.cnt}')
"
```

### 2.5 Akses Web UI

| UI | URL | Login |
|---|---|---|
| Spark Master | http://localhost:8080 | — |
| Spark Worker | http://localhost:8081 | — |
| Airflow | http://localhost:8082 | admin / admin |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | — |

---

## 3. Menghentikan Pipeline

```bash
# Stop container (data tetap aman)
docker compose down

# Stop + hapus semua data (Kafka offset + checkpoint)
docker compose down -v

# Stop service tertentu
docker compose stop producer
docker compose stop spark-processor
```

---

## 4. Update Code & Rebuild

```bash
# 1. Edit code
nano producer.py           # atau spark_processor.py

# 2. Build ulang untuk service tertentu
docker compose up --build -d producer
docker compose up --build -d spark-processor

# 3. Build ulang SEMUA service
docker compose up --build -d
```

---

## 5. Troubleshooting

### 5.1 BigQuery Data Kosong

```bash
# Cek producer log — apakah ada data dikirim?
docker logs idx-producer --tail 10

# Cek spark log — apakah ada error?
docker logs idx-spark-processor --tail 20

# Cek Kafka offset — apakah ada message?
docker exec idx-kafka kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server kafka:29092 \
  --topic idx_sector_ticks --time -1

# Reset dedup cache (restart producer)
docker compose restart producer
```

### 5.2 Spark Error: GCS Permission

> **Catatan:** Pipeline menggunakan `direct` write method (BigQuery Storage API), jadi GCS bucket tidak diperlukan untuk write data. Error ini hanya muncul jika `writeMethod` diubah ke `indirect`.

Error: `403 Forbidden` atau `Connection refused`

**Penyebab:** Service account tidak punya akses ke GCS bucket.

**Solusi:**
1. Buka GCP Console → Cloud Storage → Buckets
2. Klik bucket → PERMISSIONS → GRANT ACCESS
3. Add: `idx-pipeline-owner@...iam.gserviceaccount.com`
4. Role: `Storage Object Admin`

### 5.3 Spark Error: BigQuery Write Failed

```bash
# Delete checkpoint & restart
docker compose stop spark-processor
docker volume rm spark_checkpoints
docker compose up --build -d spark-processor
```

### 5.4 Producer: "no new candles" Terus

**Normal.** Dedup aktif — hanya kirim candle baru dari yfinance. Kalau yfinance belum update data, akan skip terus.

**Cek kapan terakhir ada candle baru:**
```sql
SELECT MAX(timestamp) FROM `idx-analytics-platform.idx_stock_data.top_sector_ticks`;
```

**Force re-fetch (reset cache):**
```bash
docker compose restart producer
```

### 5.5 Kafka Connection Error

```bash
# Cek Kafka log
docker logs idx-kafka --tail 20

# Test koneksi dari container
docker exec busybox telnet kafka 29092

# Restart Kafka
docker compose restart kafka
```

### 5.6 Container Loop Restart

```bash
# Cek log container yang restart
docker logs idx-spark-processor --tail 50

# Fix common issues:
# - GCP credential expired → download new SA key
# - Bucket tidak ada → create bucket
# - Out of memory → tambah mem_limit di docker-compose.yml
```

---

## 6. dbt Operations

### 6.1 Install dbt (Lokal)

```bash
pip install dbt-bigquery
```

> **Catatan Python 3.14:** Jika error, gunakan virtual environment atau Python 3.11/3.12.

### 6.2 Setup Profile

```bash
cd dbt
# Generate profiles.yml dari service account
python3 -c "
import json
with open('../gcp-service-account.json') as f: sa = json.load(f)
with open('profiles.yml', 'w') as f:
    f.write('''idx_stream:
  outputs:
    dev:
      type: bigquery
      method: service-account-json
      project: \"' + sa['project_id'] + '\"
      dataset: idx_stock_data
      location: asia-southeast2
      keyfile_json: ' + json.dumps(sa) + '
      threads: 4
  target: dev
''')
print('profiles.yml created')
"
```

### 6.3 Run dbt

```bash
cd dbt
dbt deps                      # Install packages (pertama kali)
dbt run                       # Run all models
dbt run --select staging      # Run specific layer
dbt run --select mrt_top5_sector_daily  # Run one model
dbt test                      # Run data quality tests
dbt docs generate             # Generate documentation
dbt docs serve                # Serve docs locally (port 8080)
```

### 6.4 Create Tables Directly (Alternatif)

Kalau dbt error, bisa create manual via BigQuery API:

```bash
python3 scripts/create_dbt_tables.py
```

---

## 7. Testing

### 7.1 Install Test Dependencies

```bash
pip install -r requirements-test.txt
```

### 7.2 Run All Tests

```bash
# Producer tests (14 test cases)
python3 -m pytest tests/test_producer.py -v

# Spark tests (8 test cases — skip jika tanpa Java)
python3 -m pytest tests/test_spark_processor.py -v

# With coverage
python3 -m pytest tests/ -v --cov=producer --cov=spark_processor
```

### 7.3 Run Specific Test

```bash
python3 -m pytest tests/test_producer.py::TestFormatTs -v
python3 -m pytest tests/test_producer.py::TestBuildPayload::test_valid_row -v
```

---

## 8. Data Quality

### 8.1 Buat Tabel DQ

```bash
python3 -c "
import os; os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'gcp-service-account.json'
from google.cloud import bigquery
client = bigquery.Client(project='idx-analytics-platform')
with open('sql/dq_check_results_ddl.sql') as f:
    client.query(f.read()).result()
print('DQ table created')
"
```

### 8.2 Jalankan DQ Checks

```bash
# Run via BigQuery console — copy paste sql/dq_checks.sql
# Atau via command line:
python3 -c "
import os; os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'gcp-service-account.json'
from google.cloud import bigquery
client = bigquery.Client(project='idx-analytics-platform')
checks = open('sql/dq_checks.sql').read().split(';')
for check in checks:
    if check.strip():
        job = client.query(check + ';')
        job.result()
        for row in job:
            print(dict(row))
"
```

---

## 9. Monitoring (Prometheus + Grafana)

### 9.1 Start Monitoring Stack

Tambahkan ke `docker-compose.yml`:

```yaml
prometheus:
  image: prom/prometheus:v2.52.0
  container_name: idx-prometheus
  ports:
    - "9090:9090"
  volumes:
    - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml

grafana:
  image: grafana/grafana:10.4.0
  container_name: idx-grafana
  ports:
    - "3000:3000"
  volumes:
    - ./monitoring/grafana/:/etc/grafana/provisioning/
```

```bash
docker compose up -d prometheus grafana
```

### 9.2 Akses

| Service | URL | Keterangan |
|---|---|---|
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | Query metrics |

---

## 10. Airflow Orchestration

### 10.1 Start Airflow

Tambahkan ke `docker-compose.yml`:

```yaml
airflow:
  image: bitnami/airflow:2.9.0
  container_name: idx-airflow
  ports:
    - "8082:8080"
  volumes:
    - ./airflow/dags:/opt/bitnami/airflow/dags
```

```bash
docker compose up -d airflow
```

### 10.2 Trigger DAG

Manual trigger via UI: http://localhost:8082
Login: admin / admin → DAGs → idx_pipeline_daily → Trigger DAG

---

## 11. Terraform (Infrastructure as Code)

### 11.1 Setup

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
nano terraform.tfvars   # Isi project_id
```

### 11.2 Apply

```bash
terraform init
terraform plan
terraform apply
```

---

## 12. Utility Commands

```bash
# Lihat Kafka messages (real-time)
docker exec idx-kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic idx_sector_ticks \
  --from-beginning \
  --max-messages 5

# Lihat Kafka messages di DLQ topic
docker exec idx-kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic idx_sector_ticks_dlq \
  --from-beginning \
  --max-messages 5

# Reset BigQuery checkpoint (force reprocess)
docker compose stop spark-processor
docker run --rm -v spark_checkpoints:/tmp/checkpoint alpine rm -rf /tmp/checkpoint/idx_stock
docker compose up --build -d spark-processor

# Reset SEMUA checkpoint + data Kafka
docker compose down -v
docker compose up --build -d

# Cek isi volume checkpoint
docker run --rm -v spark_checkpoints:/tmp/checkpoint alpine ls -la /tmp/checkpoint/

# Cek resource usage container
docker stats idx-producer idx-spark-processor idx-kafka
```

---

## 13. File Penting

| File | Fungsi | Perlu Di-edit? |
|---|---|---|
| `.env` | Konfigurasi rahasia (gitignored) | ✅ Ya — isi GCP, Kafka |
| `gcp-service-account.json` | Credentials GCP (gitignored) | ✅ Ya — letakkan file |
| `docker-compose.yml` | Orkestrasi container | ❌ Tidak (default OK) |
| `producer.py` | Logic fetch + dedup + off-hours | ❌ Tidak perlu |
| `spark_processor.py` | Logic streaming + clean + BQ write | ❌ Tidak perlu |
| `dbt/profiles.yml` | Koneksi dbt ke BigQuery | ✅ Ya — generate dari SA |
| `schemas/stock_tick_v*.avsc` | Schema contract | ⚠️ Hanya jika evolusi |
| `terraform/terraform.tfvars` | Variable Terraform | ✅ Ya — isi project_id |
