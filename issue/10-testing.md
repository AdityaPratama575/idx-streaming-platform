# Issue 10: Testing — Unit & Integration Tests

## Tujuan
Menambahkan test suite komprehensif untuk semua komponen pipeline: producer, Spark processor, dan integrasi end-to-end.

---

## Spesifikasi

### A. Producer Tests (`tests/test_producer.py`)

Gunakan **pytest** + **unittest.mock**.

| Test Case | Deskripsi |
|---|---|
| `test_init_kafka_producer_retries_on_failure` | Mock `KafkaProducer` gagal 2x lalu sukses; verifikasi retry loop dan sleep |
| `test_init_kafka_producer_connected` | Verifikasi `bootstrap_connected()` dipanggil dan producer dikembalikan |
| `test_build_payload_valid_row` | Build payload dari mock row; verifikasi semua 9 field ada dan tipe benar |
| `test_build_payload_nan_values` | Row dengan NaN → `open`/`high`/`low`/`close` jadi `None` |
| `test_build_payload_empty_volume` | Volume NaN → `None` |
| `test_format_ts_timezone_naive` | Timestamp tanpa tz → dilokalkan ke Asia/Jakarta → UTC |
| `test_format_ts_timezone_aware` | Timestamp dengan tz → langsung convert ke UTC |
| `test_format_ts_strips_microseconds` | Microseconds di-strip dari output ISO |
| `test_fetch_and_publish_success` | Mock `yf.Ticker().history()` return DataFrame; verifikasi `producer.send()` dipanggil untuk setiap row |
| `test_fetch_and_publish_empty_data` | DataFrame kosong → skip, tidak ada send |
| `test_fetch_and_publish_retry_then_success` | Gagal 2x, sukses di attempt ke-3 |
| `test_fetch_and_publish_all_retries_exhausted` | Gagal 3x → skip ticker, lanjut ke ticker berikutnya |
| `test_main_reconnect_on_kafka_failure` | `fetch_and_publish()` throw exception → reconnect dipanggil |
| `test_flush_per_sector` | Verifikasi `producer.flush()` dipanggil setiap selesai 1 sektor |
| `test_yfinance_delay_between_tickers` | Verifikasi `time.sleep()` dipanggil dengan `YFINANCE_DELAY_SECONDS` |

### B. Spark Processor Tests (`tests/test_spark_processor.py`)

Gunakan **pytest** + Spark local mode (`local[*]`).

| Test Case | Deskripsi |
|---|---|
| `test_schema_definition` | Verifikasi `StructType` memiliki 9 field dengan tipe yang benar |
| `test_parse_valid_json` | Kirim JSON valid ke Kafka mock → Spark parse → semua kolom tidak null |
| `test_parse_invalid_json_goes_to_dlq` | JSON malformed → masuk ke DLQ stream |
| `test_nan_handling_double_cols` | Row dengan NaN di open/high/low/close → jadi NULL |
| `test_nan_not_applied_to_volume` | Volume LongType → tidak terkena `isnan()` |
| `test_null_ticker_filtered` | Row dengan ticker null/empty → di-drop dari stream valid |
| `test_timestamp_parse_iso8601_with_tz` | `"2026-05-06T15:47:00+07:00"` → ter-parse ke TimestampType |
| `test_watermark_drop_duplicates` | Dua row dengan ticker+timestamp sama dalam window 30 menit → hanya 1 yang lolos |
| `test_credential_path_from_env` | `GOOGLE_APPLICATION_CREDENTIALS` dari env dipakai, bukan hardcode |

### C. Integration Tests (`tests/test_integration.py`)

| Test Case | Deskripsi |
|---|---|
| `test_producer_to_kafka_to_spark_e2e` | Jalankan producer → kirim 1 ticker → Spark baca dari Kafka memory → verifikasi output |

### D. Config

```
tests/
├── conftest.py             # Shared fixtures (SparkSession, mock Kafka)
├── test_producer.py
├── test_spark_processor.py
├── test_integration.py
└── fixtures/
    ├── sample_payload.json
    └── malformed_payload.json
```

Dependency tambahan di `requirements-test.txt`:
```
pytest>=8.0.0
pytest-mock>=3.12.0
pytest-cov>=5.0.0
coverage>=7.0.0
```

---

## Acceptance Criteria

- [ ] Semua test case di atas terimplementasi
- [ ] `pytest` berhasil dengan coverage ≥ 80%
- [ ] Test bisa jalan tanpa GCP credentials (mock semua koneksi eksternal)
- [ ] Integration test menggunakan Kafka in-memory atau Testcontainers
- [ ] Tidak ada regresi pada pipeline existing saat test dijalankan
