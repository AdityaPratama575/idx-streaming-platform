# Issue 14: Data Quality Framework

## Tujuan
Membangun layer data quality untuk mendeteksi anomali, inkonsistensi, dan data corruption di setiap stage pipeline: ingestion (producer), processing (Spark), dan storage (BigQuery/dbt).

---

## Spesifikasi

### A. Data Quality di Spark Processor (in-stream DQ checks)

Tambahkan validasi di `spark_processor.py` sebelum write ke BigQuery:

#### Check 1: OHLCV Logic Validation

```python
# Close harus di antara Low dan High (atau toleransi 0.1%)
from pyspark.sql.functions import abs as spark_abs

dq_ohlc = cleaned_df.filter(
    (col("close") < col("low") * 0.999) |
    (col("close") > col("high") * 1.001)
)
# Tulis ke DQ alert topic atau log
```

#### Check 2: Volume Non-Negative

```python
dq_negative_volume = cleaned_df.filter(col("volume") < 0)
```

#### Check 3: Timestamp Freshness

```python
# Data tidak boleh > 1 jam dari real-time
dq_stale = cleaned_df.filter(
    col("timestamp") < current_timestamp() - expr("INTERVAL 1 HOUR")
)
```

#### Check 4: Schema Completeness

```python
# Hitung jumlah null per kolom per batch
null_counts = cleaned_df.agg(*[
    count(when(col(c).isNull(), c)).alias(f"{c}_null_count")
    for c in cleaned_df.columns
])
```

#### Check 5: Sector Coverage

```python
# Semua sektor harus ada data dalam setiap batch
expected_sectors = list(top5_saham_ihsg_by_sector_market_cap.keys())
# Bandingkan dengan distinct sectors di batch ini
```

Semua hasil DQ check ditulis ke:
- **Kafka topic** `idx_data_quality_events` — untuk real-time alerting
- **BigQuery table** `dq_check_results` — untuk historical analysis

### B. Data Quality di dbt (post-load DQ tests)

Lihat Issue 12 untuk detail dbt tests. Ringkasan:

| Layer | Tests |
|---|---|
| Staging | `not_null`, `unique`, `accepted_values` |
| Intermediate | `dbt_utils.expression_is_true`, referential integrity |
| Marts | Row count comparison vs staging, anomaly detection |

### C. Data Quality Metrics Table (BigQuery)

Schema:

```sql
CREATE TABLE IF NOT EXISTS dq_check_results (
    check_id        STRING,
    check_name      STRING,
    stage           STRING,       -- 'spark_inflight', 'dbt_postload'
    batch_id        STRING,       -- Spark batch ID atau dbt run ID
    execution_ts    TIMESTAMP,
    total_rows      INT64,
    failed_rows     INT64,
    failure_rate    FLOAT64,
    details         STRING,       -- JSON detail failure
    severity        STRING        -- 'critical', 'warning', 'info'
);
```

### D. dbt Model: `int_data_quality_summary`

```sql
-- Agregasi harian DQ:
--   failure_rate per check
--   trend: apakah failure rate naik/turun
--   top 3 failing checks
```

### E. Slack / Email Alert Integration (future)

- DQ failure rate > threshold → kirim alert ke Slack channel `#data-alerts`
- Bisa diintegrasikan via Grafana alert (Issue 13) atau dbt Cloud webhook

---

## Acceptance Criteria

- [ ] 5 DQ checks berjalan di Spark processor setiap batch
- [ ] Hasil DQ check ditulis ke Kafka topic `idx_data_quality_events` dan BigQuery `dq_check_results`
- [ ] dbt tests (not_null, unique, accepted_values) pass di staging layer
- [ ] Custom DQ test (OHLCV logic) pass di intermediate layer
- [ ] Tidak ada false positive yang membanjiri DQ log
- [ ] Dashboard Grafana menampilkan DQ metrics (failure rate over time)
