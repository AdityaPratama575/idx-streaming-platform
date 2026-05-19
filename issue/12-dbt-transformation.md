# Issue 12: dbt Transformation Layer

## Tujuan
Membangun transformation layer menggunakan dbt (data build tool) di atas tabel `top_sector_ticks` di BigQuery untuk menghasilkan model analitik yang siap digunakan oleh dashboard atau analyst.

---

## Spesifikasi

### A. Struktur dbt Project

```
dbt/
├── dbt_project.yml
├── profiles.yml.template           # Template (credential diisi via env var)
├── packages.yml                     # dbt packages
├── macros/
│   ├── utils.sql                    # Helper macros (format IDR, sector classification)
│   └── schema_tests.sql             # Custom generic tests
├── models/
│   ├── staging/
│   │   ├── _sources.yml             # Source definitions
│   │   ├── stg_idx_sector_ticks.sql # Staging: rename, cast, clean raw table
│   │   └── stg_idx_sector_ticks.yml # Tests + documentation untuk staging
│   ├── intermediate/
│   │   ├── int_daily_stock_stats.sql    # Daily OHLCV aggregation per ticker
│   │   ├── int_sector_daily_summary.sql # Sector-level daily aggregates
│   │   └── int_ticker_rankings.sql      # Ticker rankings by volume/price change
│   └── marts/
│       ├── mrt_top5_sector_daily.sql    # Top 5 saham per sektor (mirip output producer)
│       ├── mrt_sector_performance.sql   # Sektor mana yang outperform hari ini
│       ├── mrt_market_breadth.sql       # Market breadth: berapa saham naik vs turun
│       └── mrt_volume_anomalies.sql     # Anomali volume (2x std dev dari avg 7 hari)
├── seeds/
│   └── sector_mapping.csv              # Mapping sektor IDX → kategori
├── snapshots/
│   └── idx_ticker_snapshot.sql         # SCD Type 2 untuk ticker list
└── analyses/
    └── ad_hoc_exploration.sql          # Analisis ad-hoc
```

### B. Model Spesifikasi Detail

#### Staging: `stg_idx_sector_ticks`

```sql
-- Rename colonnes ke snake_case standar
-- Cast tipe yang benar
-- Filter: hapus data > 90 hari untuk performance
-- Tambahkan kolom: date_id (DATE dari timestamp), hour_bucket
```

#### Intermediate: `int_daily_stock_stats`

```sql
-- Agregasi harian per ticker:
--   open_price (first), close_price (last), high, low
--   total_volume, avg_volume_per_minute
--   price_change, price_change_pct
--   vwap (volume-weighted average price)
-- Group by: ticker, sector, date_id
```

#### Intermediate: `int_sector_daily_summary`

```sql
-- Agregasi per sektor:
--   total_volume_sektor
--   avg_price_change_pct_sektor
--   ticker_count, tickers_up, tickers_down
--   top_gainer_ticker, top_loser_ticker
```

#### Mart: `mrt_top5_sector_daily`

```sql
-- Window function: ROW_NUMBER() PARTITION BY sector, date_id ORDER BY volume DESC
-- Ambil rank 1-5 untuk setiap sektor
-- Output: ready-to-consume top 5 per sektor
```

#### Mart: `mrt_sector_performance`

```sql
-- Ranking sektor berdasarkan avg price change hari ini
-- Bandingkan dengan hari sebelumnya (WoW, MoW)
-- Output: dashboard "Sector Heatmap"
```

#### Mart: `mrt_market_breadth`

```sql
-- Hari ini: berapa saham naik, turun, flat
-- Per sektor: advance/decline ratio
-- Output: market health indicator
```

### C. Data Quality Tests (`schema.yml`)

Setiap model `.yml` harus punya:

| Jenis Test | Target | Contoh |
|---|---|---|
| `not_null` | `ticker`, `timestamp`, `close`, `volume` | Staging layer |
| `unique` | `ticker` + `timestamp` (composite) | Setelah dedup |
| `accepted_values` | `sector` | Harus match `seeds/sector_mapping.csv` |
| `dbt_utils.expression_is_true` | `close > 0`, `volume >= 0` | Data sanity |
| `dbt_expectations` | `close` between `low` and `high` | OHLCV logic check |
| Custom: `anomaly_detection` | Volume spike > 3x std dev | Mart layer |

### D. dbt Packages (`packages.yml`)

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.2.0
  - package: calogica/dbt_expectations
    version: 0.10.0
  - package: dbt-labs/audit_helper
    version: 0.11.0
```

### E. Materialization Strategy

| Layer | Materialization | Reason |
|---|---|---|
| Staging | `view` | Selalu fresh dari source |
| Intermediate | `table` | Agregasi, dipakai oleh multiple mart |
| Marts | `table` | Performance query dashboard |
| Snapshots | `snapshot` | Track perubahan ticker list |

### F. dbt docs

- Generate `dbt docs generate` dan serve via GitHub Pages
- Setiap model harus punya `description:` di `.yml`
- Tambahkan `meta` tag untuk ownership, SLA, sensitivity

---

## Acceptance Criteria

- [ ] `dbt run` berhasil untuk semua model (staging → intermediate → marts)
- [ ] `dbt test` pass 100% untuk semua generic + custom tests
- [ ] `dbt docs generate` menghasilkan dokumentasi yang bisa di-browse
- [ ] Semua model memiliki deskripsi dan column-level documentation
- [ ] Tidak ada hardcoded reference ke project/dataset name (pakai `{{ var() }}` atau `{{ target }}`)
- [ ] Incremental model digunakan untuk tabel yang besar
- [ ] Profiling query: semua model selesai dalam < 5 menit
