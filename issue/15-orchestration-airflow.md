# Issue 15: Orchestration — Airflow DAG

## Tujuan
Mengganti while-loop sederhana di producer dengan Apache Airflow DAG untuk orchestration yang proper: scheduling, retry, alerting, dependency management, dan monitoring.

---

## Spesifikasi

### A. Airflow Service (ditambahkan ke `docker-compose.yml`)

Gunakan **Bitnami Airflow** image (all-in-one: webserver, scheduler, worker) untuk development:

```yaml
airflow:
  image: bitnami/airflow:2.9.0
  container_name: idx-airflow
  ports:
    - "8082:8080"
  environment:
    AIRFLOW_LOAD_EXAMPLES: "no"
    AIRFLOW_USERNAME: admin
    AIRFLOW_PASSWORD: ${AIRFLOW_PASSWORD:-admin}
  volumes:
    - ./airflow/dags:/opt/bitnami/airflow/dags
    - ./airflow/plugins:/opt/bitnami/airflow/plugins
    - ./airflow/requirements.txt:/bitnami/python/requirements.txt
    - airflow_data:/opt/bitnami/airflow
  networks:
    - idx-network
  restart: on-failure
```

### B. DAG: `idx_pipeline_daily.py`

**Schedule:** `@daily` (setiap hari jam 8:00 WIB / 01:00 UTC)

**Tasks:**

```
[start] → [fetch_stock_data] → [wait_for_spark] → [dbt_run] → [dbt_test] → [data_quality_report] → [end]
                                ↘                ↘
                           [slack_notify     [slack_notify
                            on_failure]       on_failure]
```

#### Task 1: `fetch_stock_data` (KubernetesPodOperator / DockerOperator)

- Jalankan producer container
- Parameter: `FETCH_INTERVAL_SECONDS=3600` (sekali fetch per hari, bukan loop)
- Atau gunakan `PythonOperator` yang meng-import dan memanggil `producer.main()` langsung

#### Task 2: `wait_for_spark` (Sensor)

- Poll BigQuery: apakah ada data baru dalam 10 menit terakhir?
- Timeout: 30 menit
- Poke interval: 2 menit

#### Task 3: `dbt_run` (BashOperator)

```bash
cd /opt/airflow/dbt && dbt run --profiles-dir . --target prod
```

#### Task 4: `dbt_test` (BashOperator)

```bash
cd /opt/airflow/dbt && dbt test --profiles-dir . --target prod
```

#### Task 5: `data_quality_report` (PythonOperator)

- Query `dq_check_results` table di BigQuery
- Generate summary: passed checks, failed checks, failure trend
- Kirim ke Slack/email jika ada critical failure

### C. DAG: `idx_streaming_monitor.py`

**Schedule:** `*/10 * * * *` (setiap 10 menit)

**Tasks:**

```
[check_kafka_lag] → [check_bigquery_freshness] → [check_container_health]
```

#### Task 1: `check_kafka_lag`

- Query Kafka consumer group offset
- Alert jika lag > threshold

#### Task 2: `check_bigquery_freshness`

```sql
SELECT MAX(timestamp) FROM `top_sector_ticks`
```
- Alert jika timestamp terbaru > 15 menit dari `CURRENT_TIMESTAMP()`

#### Task 3: `check_container_health`

- Docker health check via `docker ps` filter status != running
- Alert jika ada container down

### D. Airflow Connections yang Dibutuhkan

| Connection ID | Type | Deskripsi |
|---|---|---|
| `gcp_bigquery` | Google Cloud | Untuk BigQuery sensor dan query ops |
| `slack_default` | Slack Webhook | Untuk alert notification |

### E. File Structure

```
airflow/
├── dags/
│   ├── idx_pipeline_daily.py
│   └── idx_streaming_monitor.py
├── plugins/
│   └── idx_custom_operators.py      # Custom Sensor untuk BigQuery freshness
├── dbt/
│   └── (symlink atau copy dari /dbt/)
└── requirements.txt
```

---

## Acceptance Criteria

- [ ] Airflow webserver bisa diakses di `http://localhost:8082`
- [ ] DAG `idx_pipeline_daily` berhasil run end-to-end (manual trigger)
- [ ] DAG `idx_streaming_monitor` berjalan otomatis setiap 10 menit
- [ ] Retry logic jalan: task gagal → auto-retry 3x dengan exponential backoff
- [ ] Failure alert terkirim ke Slack (skipped jika Slack tidak dikonfigurasi, cukup log)
- [ ] Semua DAG tasks idempotent (bisa di-rerun tanpa efek samping)
