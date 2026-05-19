# Issue 13: Monitoring & Observability

## Tujuan
Menambahkan monitoring stack (Prometheus + Grafana) untuk memonitor kesehatan pipeline, metrik Kafka, metrik Spark, dan aplikasi metrics kustom.

---

## Spesifikasi

### A. Monitoring Stack Services (ditambahkan ke `docker-compose.yml`)

#### Prometheus

```yaml
prometheus:
  image: prom/prometheus:v2.52.0
  container_name: idx-prometheus
  ports:
    - "9090:9090"
  volumes:
    - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml
    - prometheus_data:/prometheus
  command:
    - '--config.file=/etc/prometheus/prometheus.yml'
    - '--storage.tsdb.retention.time=15d'
  networks:
    - idx-network
  restart: on-failure
```

#### Grafana

```yaml
grafana:
  image: grafana/grafana:10.4.0
  container_name: idx-grafana
  ports:
    - "3000:3000"
  environment:
    GF_SECURITY_ADMIN_USER: admin
    GF_SECURITY_ADMIN_PASSWORD: ${GRAFANA_PASSWORD:-admin}
  volumes:
    - ./monitoring/grafana/dashboards:/etc/grafana/provisioning/dashboards
    - ./monitoring/grafana/datasources:/etc/grafana/provisioning/datasources
    - grafana_data:/var/lib/grafana
  networks:
    - idx-network
  restart: on-failure
```

### B. Metrik yang Harus Di-expose

#### Producer Metrics (custom, via `prometheus_client`)

| Metrik | Tipe | Deskripsi |
|---|---|---|
| `idx_messages_sent_total` | Counter | Total message terkirim ke Kafka |
| `idx_fetch_errors_total` | Counter | Jumlah gagal fetch dari yfinance |
| `idx_fetch_duration_seconds` | Histogram | Durasi 1 batch fetch cycle |
| `idx_tickers_succeeded` | Gauge | Jumlah ticker berhasil di-fetch dalam batch terakhir |
| `idx_tickers_failed` | Gauge | Jumlah ticker gagal dalam batch terakhir |
| `idx_kafka_connection_status` | Gauge | 0 atau 1 (connected/tidak) |

#### Spark Metrics (expose via JMX Prometheus Exporter)

| Metrik | Sumber | Deskripsi |
|---|---|---|
| `spark_streaming_input_rate` | Spark JMX | Records/sec dari Kafka |
| `spark_streaming_processing_rate` | Spark JMX | Records/sec diproses |
| `spark_streaming_batch_duration` | Spark JMX | Durasi per microbatch |
| `spark_streaming_lag` | Spark JMX | Lag offset Kafka |
| `spark_executor_memory_used` | Spark JMX | Memory usage executor |

#### Kafka Metrics (expose via JMX Prometheus Exporter)

| Metrik | Deskripsi |
|---|---|
| `kafka_consumer_lag` | Consumer group lag |
| `kafka_messages_in_per_sec` | Messages/sec ke topic |
| `kafka_bytes_in_per_sec` | Bytes/sec ke broker |
| `kafka_under_replicated_partitions` | Partisi under-replicated (should be 0) |

### C. Grafana Dashboards

Buat 3 dashboard pre-configured:

#### Dashboard 1: Pipeline Overview

- Row 1: Messages/sec (time series), Kafka lag (gauge), Pipeline status (up/down)
- Row 2: Fetch success rate (%), Error rate over time
- Row 3: Spark batch duration (histogram), Processing lag

#### Dashboard 2: Data Quality

- Row 1: Messages total vs DLQ messages (comparison)
- Row 2: DLQ rate (%), Ticker coverage (% tickers reporting data)
- Row 3: Timestamp freshness (max timestamp in BigQuery vs now)

#### Dashboard 3: Infrastructure Health

- Row 1: CPU usage per container, Memory usage per container
- Row 2: Kafka broker health, partition status
- Row 3: Docker container status (running/stopped)

### D. Alerting Rules (`monitoring/prometheus.yml`)

```yaml
rule_files:
  - alerts.yml

groups:
  - name: idx_pipeline_alerts
    rules:
      - alert: PipelineNoData
        expr: rate(idx_messages_sent_total[5m]) == 0
        for: 10m
        labels:
          severity: critical
        annotations:
          summary: "Tidak ada data masuk ke Kafka selama 10 menit"

      - alert: HighDLQRate
        expr: rate(idx_dlq_messages_total[5m]) / rate(idx_messages_sent_total[5m]) > 0.05
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "DLQ rate > 5% — kemungkinan schema drift atau data corruption"

      - alert: KafkaConsumerLag
        expr: kafka_consumer_lag > 10000
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Spark consumer tertinggal > 10k messages dari Kafka"

      - alert: ServiceDown
        expr: up == 0
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "Service {{ $labels.job }} is down"
```

### E. File Structure

```
monitoring/
├── prometheus.yml
├── alerts.yml
└── grafana/
    ├── dashboards/
    │   ├── pipeline_overview.json
    │   ├── data_quality.json
    │   └── infrastructure_health.json
    └── datasources/
        └── prometheus.yml
```

---

## Acceptance Criteria

- [ ] Prometheus terkoneksi ke semua exporter (producer, Spark JMX, Kafka JMX)
- [ ] Grafana menampilkan 3 dashboard dengan data real-time
- [ ] Semua 4 alerting rules aktif dan terverifikasi trigger
- [ ] Producer meng-export custom metrics via HTTP endpoint
- [ ] Semua container ter-monitor resource usage-nya
- [ ] Dashboard Grafana tersimpan sebagai code (JSON provisioning)
