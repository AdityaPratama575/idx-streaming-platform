# Issue 21: Data Catalog & Lineage — dbt Docs + OpenLineage

## Tujuan
Membangun data discovery layer: data catalog untuk dokumentasi tabel, data lineage untuk trace alur data, dan self-service documentation untuk analyst/stakeholder.

---

## Spesifikasi

### A. dbt Docs (Data Catalog Lightweight)

Generate static documentation dari dbt project (Issue 12):

```bash
# Generate docs
dbt docs generate

# Serve locally
dbt docs serve --port 8083
```

#### Hosting: GitHub Pages

Tambahkan GitHub Actions workflow untuk auto-publish:

```yaml
# .github/workflows/dbt-docs.yml
name: Publish dbt Docs

on:
  push:
    branches: [main]
    paths:
      - 'dbt/**'

jobs:
  docs:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Setup Python
        uses: actions/setup-python@v5
        with: { python-version: '3.9' }
      
      - name: Install dbt
        run: pip install dbt-bigquery
      
      - name: Generate dbt Docs
        env:
          GCP_SERVICE_ACCOUNT_JSON: ${{ secrets.GCP_SERVICE_ACCOUNT_JSON }}
        run: |
          echo "$GCP_SERVICE_ACCOUNT_JSON" > /tmp/sa.json
          cd dbt && dbt docs generate --profiles-dir .
      
      - name: Deploy to GitHub Pages
        uses: peaceiris/actions-gh-pages@v3
        with:
          github_token: ${{ secrets.GITHUB_TOKEN }}
          publish_dir: dbt/target
          destination_dir: dbt-docs
```

URL: `https://adityapratama575.github.io/idx-streaming-platform/dbt-docs/`

### B. Data Dictionary (Manual, Complement untuk dbt Docs)

Buat `docs/data_dictionary.md`:

```markdown
# Data Dictionary — IDX-Stream

## Raw Layer: `top_sector_ticks`
| Column | Type | Description | Source | Nullable |
|---|---|---|---|---|
| ticker | STRING | Stock ticker code (e.g., BBCA.JK) | yfinance | No |
| sector | STRING | IDX business sector | hardcoded mapping | No |
| timestamp | TIMESTAMP | Trade timestamp (UTC) | yfinance DatetimeIndex | No |
| fetch_ts | TIMESTAMP | System fetch timestamp (UTC) | Python datetime.now() | No |
| open | FLOAT64 | Opening price | yfinance | Yes |
| high | FLOAT64 | Highest price in interval | yfinance | Yes |
| low | FLOAT64 | Lowest price in interval | yfinance | Yes |
| close | FLOAT64 | Closing price | yfinance | Yes |
| volume | INT64 | Trading volume | yfinance | Yes |

## Marts Layer
... (lanjutkan untuk semua tabel di Issue 18)
```

### C. OpenLineage Integration (Data Lineage)

Integrasikan OpenLineage dengan Spark via `openlineage-spark` JAR:

#### Dockerfile.spark Update

```dockerfile
# Download OpenLineage Spark listener
RUN curl -fsSL -o /opt/spark/jars/openlineage-spark_2.12-1.12.0.jar \
    https://repo1.maven.org/maven2/io/openlineage/openlineage-spark_2.12/1.12.0/openlineage-spark_2.12-1.12.0.jar
```

#### Spark Session Config

```python
spark = SparkSession.builder \
    .appName(SPARK_APP_NAME) \
    .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener") \
    .config("spark.openlineage.transport.type", "console") \  # Dev
    # .config("spark.openlineage.transport.type", "http") \   # Production
    # .config("spark.openlineage.transport.url", "http://marquez:5000/api/v1") \
    .getOrCreate()
```

#### Lineage Service: Marquez (optional, nice-to-have)

Tambahkan ke `docker-compose.yml`:

```yaml
marquez:
  image: marquezproject/marquez:0.45.0
  container_name: idx-marquez
  ports:
    - "5000:5000"
    - "5001:5001"
  environment:
    MARQUEZ_DB: /opt/marquez/data/marquez.db
  volumes:
    - marquez_data:/opt/marquez/data
  networks:
    - idx-network

marquez-web:
  image: marquezproject/marquez-web:0.45.0
  container_name: idx-marquez-web
  ports:
    - "3001:3000"
  environment:
    MARQUEZ_HOST: marquez
    MARQUEZ_PORT: 5000
  networks:
    - idx-network
```

### D. Lineage Graph yang Diharapkan

```
[yfinance API]
      │
      ▼
[producer.py] ────► [Kafka: idx_sector_ticks]
                            │
                            ▼
                    [spark_processor.py]
                      │            │
                      ▼            ▼
              [BigQuery:     [Kafka: idx_sector_ticks_dlq]
               top_sector_ticks]
                      │
                      ▼
              [dbt: stg_idx_sector_ticks]
                      │
              ┌───────┼───────┬──────────────┐
              ▼       ▼       ▼              ▼
          [int_*] [agg_*]  [mrt_*]     [dim_*]
              │       │       │              │
              └───────┴───────┴──────────────┘
                      │
                      ▼
              [dbt test results]
```

### E. dbt Docs Enrichment

Di `dbt_project.yml`:

```yaml
models:
  idx_stream:
    staging:
      +tags: ["staging", "raw"]
      +meta:
        owner: "data_engineering"
        sla: "15_minutes_from_ingestion"
    intermediate:
      +tags: ["intermediate", "aggregated"]
      +meta:
        owner: "data_engineering"
        sensitivity: "internal"
    marts:
      +tags: ["marts", "dashboard_ready"]
      +meta:
        owner: "analytics_team"
        sensitivity: "internal"
        exposure: "tableau_dashboard"
```

### F. File Structure

```
docs/
├── data_dictionary.md
├── data_model.md          # ERD + table descriptions (from Issue 18)
├── lineage.md             # Opens the full lineage diagram (hardcopy)
└── architecture.md        # Architecture decision records (ADRs)
```

---

## Acceptance Criteria

- [ ] `dbt docs generate` menghasilkan dokumentasi yang lengkap
- [ ] `dbt docs` di-publish otomatis ke GitHub Pages via GitHub Actions
- [ ] `docs/data_dictionary.md` mendokumentasikan semua tabel + kolom di semua layer
- [ ] OpenLineage Spark listener terkonfigurasi dan mengirim lineage events
- [ ] Lineage graph (hardcopy) tersedia di `docs/lineage.md`
- [ ] dbt models memiliki `meta` tags untuk ownership dan SLA
- [ ] Data catalog bisa diakses oleh non-engineer (analyst, PM) via browser
