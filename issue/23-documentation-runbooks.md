# Issue 23: Documentation & Runbooks

## Tujuan
Melengkapi dokumentasi operasional: runbook untuk incident response, Architecture Decision Records (ADRs), data flow runbook, dan on-call guide.

---

## Spesifikasi

### A. Architecture Decision Records (ADRs)

Buat `docs/adr/` directory dengan keputusan arsitektur yang sudah ada:

| ADR | Title | Status |
|---|---|---|
| ADR-001 | Use Kafka KRaft mode instead of Zookeeper | Accepted |
| ADR-002 | Use BigQuery Storage Write API (direct mode) vs batch load | Accepted |
| ADR-003 | Docker Compose for dev, Kubernetes for production | Accepted |
| ADR-004 | Python kafka-python vs confluent-kafka | Accepted |
| ADR-005 | Spark cluster mode (master+worker) vs local mode | Accepted |
| ADR-006 | dbt for transformation vs stored procedures | Accepted |
| ADR-007 | Schema contract with versioning vs Schema Registry | Accepted (Issue 17) |
| ADR-008 | Airflow for orchestration vs Prefect vs Dagster | Accepted (Issue 15) |

Format ADR:

```markdown
# ADR-001: Use Kafka KRaft mode instead of Zookeeper

## Status
Accepted (May 2026)

## Context
Kafka traditionally requires a separate Zookeeper cluster for metadata management. 
For a single-node local pipeline, Zookeeper adds overhead (extra container, extra 
memory, extra failure surface).

## Decision
Use Kafka KRaft (KIP-500) mode which eliminates Zookeeper dependency.

## Consequences
- **Positive:** 1 fewer container, ~500MB less RAM, simpler docker-compose
- **Positive:** Production upgrade path to Kraft-based cluster
- **Negative:** KRaft was relatively new (mature in Kafka 3.5+)
- **Negative:** Some legacy tooling assumes Zookeeper
```

### B. Runbook: Incident Response

Buat `docs/runbooks/`:

#### `runbooks/pipeline_down.md`

```markdown
# Runbook: Pipeline Tidak Mengirim Data ke BigQuery

## Severity: P0 (Critical)

## Symptom
- Grafana alert "PipelineNoData" triggered (>10 menit tidak ada message)
- BigQuery: `SELECT MAX(timestamp) FROM top_sector_ticks` > 15 menit dari now

## Diagnosis Steps

### 1. Cek Kafka
kubectl exec -it kafka-0 -- kafka-topics --bootstrap-server localhost:29092 --list
kubectl exec -it kafka-0 -- kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:29092 --topic idx_sector_ticks --time -1

### 2. Cek Producer
kubectl logs -f deployment/idx-producer --tail=100
# Cari error: yfinance timeout, Kafka connection refused

### 3. Cek Spark Processor
kubectl logs -f deployment/idx-spark-processor --tail=100
# Cari error: BigQuery permission, schema mismatch, OOM

### 4. Cek BigQuery
bq query --use_legacy_sql=false \
  'SELECT COUNT(*) AS row_count, MAX(timestamp) AS latest_ts 
   FROM idx_stock_data.top_sector_ticks 
   WHERE timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)'

## Resolution

### Producer Error
1. Restart: kubectl rollout restart deployment/idx-producer
2. Jika persist: cek yfinance rate limit (tunggu 15 menit)

### Spark Processor Error
1. Hapus checkpoint jika stuck: kubectl exec -it idx-spark-processor -- rm -rf /tmp/spark-checkpoints/idx_stock
2. Restart: kubectl rollout restart deployment/idx-spark-processor

### Kafka Error
1. Cek disk: kubectl exec kafka-0 -- df -h /var/lib/kafka/data
2. Jika disk penuh: hapus old topics atau perluas PVC

## Escalation
Jika > 30 menit unresolved → escalate ke on-call senior engineer
```

#### `runbooks/data_quality_alert.md`

```markdown
# Runbook: Data Quality Alert — High DLQ Rate

## Severity: P1 (High)

## Symptom
- Grafana alert "HighDLQRate" triggered (DLQ > 5% dari total messages)
- Cek: docker exec idx-kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic idx_sector_ticks_dlq --max-messages 5

## Diagnosis
1. Cek sample DLQ message: apakah ada field yang hilang/berubah nama?
2. Cek producer schema: apakah ada perubahan payload di producer.py?
3. Cek Spark schema: apakah SCHEMA di spark_processor.py masih sinkron?

## Resolution
1. Jika schema drift: update schema di kedua sisi (lihat Issue 17)
2. Jika corrupt data: cek source (yfinance API change?)
3. Temporary: restart producer dengan payload yang benar

## Post-Mortem
Buat ticket untuk root cause analysis setelah incident resolved.
```

### C. Onboarding Guide

Buat `docs/onboarding.md`:

```markdown
# Onboarding — IDX-Stream Pipeline

## Prerequisites Setup (30 menit)
1. Install: Docker, gcloud CLI, Python 3.9, Terraform
2. Clone repo: git clone git@github.com:AdityaPratama575/idx-streaming-platform.git
3. GCP access: minta service account key ke lead engineer
4. Copy .env.example ke .env, isi sesuai environment

## First Run
1. docker-compose up --build
2. Buka http://localhost:8080 (Spark UI)
3. Buka http://localhost:3000 (Grafana)
4. Verifikasi: SELECT COUNT(*) FROM idx_stock_data_dev.top_sector_ticks

## Key Contacts
- Data Engineering Lead: [name]
- GCP Admin: [name]
- On-call rotation: [link]

## Useful Links
- Spark UI: http://localhost:8080
- Kafka: localhost:9092
- Grafana: http://localhost:3000
- BigQuery Console: https://console.cloud.google.com/bigquery
- dbt Docs: https://adityapratama575.github.io/idx-streaming-platform/dbt-docs/
```

### D. API / Interface Documentation

Buat `docs/interfaces.md`:

```markdown
# Data Interfaces

## Kafka Topics

| Topic | Partition | Retention | Schema | Consumers |
|---|---|---|---|---|
| idx_sector_ticks | 3 | 7 days | stock_tick_v1 | spark_processor |
| idx_sector_ticks_dlq | 1 | 30 days | raw JSON | debugging |
| idx_data_quality_events | 1 | 30 days | dq_event_v1 | monitoring |

## BigQuery Tables

| Table | Partition | Refresh | Access |
|---|---|---|---|
| top_sector_ticks | timestamp (DAY) | Real-time (streaming) | data_engineering |
| aggr_daily_ticker | date (DAY) | Daily (dbt) | analytics_team |
| mrt_top5_sector_daily | date (DAY) | Daily (dbt) | analytics_team, tableau |
```

### E. File Structure

```
docs/
├── adr/
│   ├── 001-kafka-kraft-mode.md
│   ├── 002-bigquery-direct-write.md
│   ├── 003-docker-compose-vs-k8s.md
│   ├── 004-kafka-python-vs-confluent.md
│   ├── 005-spark-cluster-vs-local.md
│   ├── 006-dbt-vs-stored-procedures.md
│   ├── 007-schema-contract-vs-registry.md
│   └── 008-airflow-vs-prefect.md
├── runbooks/
│   ├── pipeline_down.md
│   ├── data_quality_alert.md
│   └── kafka_disk_full.md
├── onboarding.md
├── interfaces.md
└── data_dictionary.md      (from Issue 21)
```

---

## Acceptance Criteria

- [ ] 8 ADRs mendokumentasikan semua keputusan arsitektur yang sudah diambil
- [ ] Runbook `pipeline_down.md` bisa di-follow oleh engineer yang tidak familiar dengan pipeline
- [ ] Runbook `data_quality_alert.md` mencakup step diagnosis + resolusi
- [ ] `onboarding.md` memungkinkan engineer baru setup pipeline dalam < 1 jam
- [ ] `interfaces.md` mendokumentasikan semua Kafka topics + BigQuery tables
- [ ] Semua dokumen di-update secara berkala (link di README)
