# Issue 17: Schema Registry & Evolution

## Tujuan
Mengimplementasikan schema management untuk menghilangkan tight coupling antara producer dan Spark processor, serta mendukung backward-compatible schema evolution.

---

## Spesifikasi

### A. Pilihan Implementasi

Karena infrastruktur masih sederhana (single-node Kafka, Docker Compose), gunakan pendekatan **Schema Contract + Schema Versioning via Topic** sebagai langkah awal sebelum full Schema Registry.

### B. Schema Contract File

Buat file `schemas/stock_tick_v1.avsc`:

```json
{
  "type": "record",
  "name": "StockTick",
  "namespace": "com.idx.stream",
  "version": "1",
  "fields": [
    {"name": "ticker",   "type": "string"},
    {"name": "sector",   "type": "string"},
    {"name": "timestamp","type": {"type": "string", "logicalType": "iso8601"}},
    {"name": "fetch_ts", "type": {"type": "string", "logicalType": "iso8601"}},
    {"name": "open",     "type": ["null", "double"], "default": null},
    {"name": "high",     "type": ["null", "double"], "default": null},
    {"name": "low",      "type": ["null", "double"], "default": null},
    {"name": "close",    "type": ["null", "double"], "default": null},
    {"name": "volume",   "type": ["null", "long"],   "default": null}
  ]
}
```

### C. Schema Version di Kafka Record Header

Producer menambahkan header:

```python
producer.send(
    topic=KAFKA_TOPIC,
    value=payload,
    headers=[
        ("schema_name", b"stock_tick"),
        ("schema_version", b"1"),
        ("producer_version", b"1.0.0"),
    ]
)
```

Spark processor membaca header:

```python
from pyspark.sql.functions import col

# Baca headers dari Kafka record
raw_with_headers = raw_df.selectExpr(
    "CAST(value AS STRING) as json_str",
    "headers",
    "topic", "partition", "offset"
)
# Parse schema_version dari headers
```

### D. Schema Versioning Strategy

#### Menambah field baru (Forward Compatible — v1 → v2)

1. Tambah field ke `schemas/stock_tick_v2.avsc` dengan **default value**
2. Producer update: kirim field baru
3. Spark processor update: tambah field ke `StructType` dengan nullable=True
4. Deploy Spark processor **sebelum** producer (agar bisa handle field baru dengan null)

#### Contoh: Tambah `change_percent`

```json
{"name": "change_percent", "type": ["null", "double"], "default": null}
```

Producer v2 kirim `change_percent`, Spark processor v2 baca, v1 abaikan (ignor unknown fields via `option("mode", "PERMISSIVE")`).

### E. Konfigurasi Spark untuk Toleransi Schema

```python
# Di spark_processor.py
raw_with_str = raw_df.selectExpr(
    "CAST(value AS STRING) as json_str"
)
parsed_df = raw_with_str.withColumn(
    "parsed", 
    from_json(col("json_str"), SCHEMA, {"mode": "PERMISSIVE"})
)
```

- `PERMISSIVE`: field tidak dikenal → NULL (tidak error)
- `FAILFAST`: field tidak dikenal → exception (strict mode, opsional via config)

### F. Validasi Schema di CI/CD

Tambahkan step di GitHub Actions (Issue 11):

```yaml
- name: Validate Schema Sync
  run: |
    python scripts/validate_schema_sync.py
```

Script `scripts/validate_schema_sync.py`:
- Parse `schemas/stock_tick_v1.avsc`
- Parse `producer.py:build_payload()` fields
- Parse `spark_processor.py:SCHEMA` StructType
- Assert: semua field nama + tipe cocok
- Fail CI jika tidak sinkron

### G. Schema Documentation

Buat `schemas/CHANGELOG.md`:

| Version | Date | Changes |
|---|---|---|
| v1 | May 2026 | Initial schema: 9 fields (ticker, sector, timestamp, fetch_ts, OHLCV) |
| v2 | TBD | Add `change_percent` (nullable double) |
| v3 | TBD | Add `market_cap` (nullable long) |

### H. File Structure

```
schemas/
├── stock_tick_v1.avsc
├── CHANGELOG.md
└── README.md            # Schema evolution rules & FAQ
scripts/
└── validate_schema_sync.py
```

---

## Acceptance Criteria

- [ ] File `.avsc` mendefinisikan schema producer + processor secara eksplisit
- [ ] Producer mengirim `schema_name` + `schema_version` di Kafka record headers
- [ ] Spark processor membaca dan log schema version dari headers
- [ ] `from_json()` menggunakan mode `PERMISSIVE` untuk forward compatibility
- [ ] Script `validate_schema_sync.py` bisa mendeteksi mismatch schema
- [ ] CI/CD pipeline menjalankan schema validation
- [ ] `schemas/CHANGELOG.md` mendokumentasikan semua perubahan schema
