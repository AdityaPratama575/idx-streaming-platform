# Issue 22: Multi-Environment Configuration

## Tujuan
Memisahkan konfigurasi untuk environment development, staging, dan production agar pipeline bisa di-deploy ke multiple environments tanpa modifikasi kode.

---

## Spesifikasi

### A. Environment Hierarchy

```
dev (local Docker) ──► staging (GKE/dev project) ──► production (GKE/prod project)
```

### B. Perbedaan Environment

| Konfigurasi | dev | staging | production |
|---|---|---|---|
| Kafka node | 1 (KRaft single) | 1 (KRaft single) | 3 (KRaft cluster) |
| Spark workers | 1 | 2 | 4 |
| BigQuery dataset | `idx_stock_data_dev` | `idx_stock_data_staging` | `idx_stock_data` |
| GCS bucket | `*-temp-staging-dev` | `*-temp-staging-stg` | `*-temp-staging` |
| Checkpoint dir | `/tmp/spark-checkpoints/dev` | `/tmp/spark-checkpoints/staging` | `/tmp/spark-checkpoints/prod` |
| Fetch interval | 300s (5 menit) | 60s (1 menit) | 30s (real-time) |
| Log level | DEBUG | INFO | WARN |
| Auto-create topics | `true` | `false` | `false` |
| Resource limits | dev limits | medium | production limits |
| Monitoring retention | 3 hari | 7 hari | 30 hari |
| Alerts enabled | No (log only) | Yes (Slack) | Yes (Slack + PagerDuty) |

### C. `.env` File per Environment

```
.env.dev           # Local development (current .env)
.env.staging       # Staging overrides
.env.prod          # Production overrides (NEVER committed)
```

#### `.env.dev` (local)

```
ENVIRONMENT=dev
GCP_PROJECT_ID=idx-analytics-platform
GCP_BIGQUERY_DATASET=idx_stock_data_dev
KAFKA_AUTO_CREATE_TOPICS_ENABLE=true
FETCH_INTERVAL_SECONDS=300
SPARK_LOG_LEVEL=DEBUG
```

#### `.env.staging`

```
ENVIRONMENT=staging
GCP_PROJECT_ID=idx-analytics-platform-staging
GCP_BIGQUERY_DATASET=idx_stock_data_staging
KAFKA_AUTO_CREATE_TOPICS_ENABLE=false
FETCH_INTERVAL_SECONDS=60
SPARK_LOG_LEVEL=INFO
```

#### `.env.prod`

```
ENVIRONMENT=production
GCP_PROJECT_ID=idx-analytics-platform-prod
GCP_BIGQUERY_DATASET=idx_stock_data
KAFKA_AUTO_CREATE_TOPICS_ENABLE=false
FETCH_INTERVAL_SECONDS=30
SPARK_LOG_LEVEL=WARN
SPARK_WORKER_REPLICAS=4
```

### D. `docker-compose.yml` Update

```yaml
services:
  producer:
    env_file:
      - .env.${ENVIRONMENT:-dev}   # Default ke dev

  spark-processor:
    env_file:
      - .env.${ENVIRONMENT:-dev}
```

Usage:

```bash
# Dev
docker-compose up

# Staging
ENVIRONMENT=staging docker-compose up

# Production
ENVIRONMENT=prod docker-compose up
```

### E. Terraform Workspaces

```hcl
# terraform/variables.tf

variable "environment" {
  description = "Deployment environment"
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}

variable "bq_dataset_name" {
  default = {
    dev     = "idx_stock_data_dev"
    staging = "idx_stock_data_staging"
    prod    = "idx_stock_data"
  }
}
```

Usage:

```bash
# Dev
terraform workspace new dev
terraform apply -var="environment=dev"

# Staging
terraform workspace new staging
terraform apply -var="environment=staging"

# Production
terraform workspace new prod
terraform apply -var="environment=prod"
```

### F. dbt Targets

`dbt/profiles.yml`:

```yaml
idx_stream:
  target: "{{ env_var('DBT_TARGET', 'dev') }}"
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: idx-analytics-platform
      dataset: idx_stock_data_dev
      threads: 2
      
    staging:
      type: bigquery
      method: service-account
      project: idx-analytics-platform-staging
      dataset: idx_stock_data_staging
      threads: 4
      
    prod:
      type: bigquery
      method: oauth
      project: idx-analytics-platform-prod
      dataset: idx_stock_data
      threads: 8
```

### G. Helm Values per Environment (Issue 19)

```
k8s/
├── values-dev.yaml       # Resource: requests kecil, replicas: 1
├── values-staging.yaml   # Resource: medium, replicas: 2
└── values-prod.yaml      # Resource: production, replicas: 4
```

### H. Environment Promotion Workflow

```
1. Dev → local test pass
2. PR merge ke main → auto-deploy ke staging (CI/CD)
3. Smoke test di staging (query BigQuery, cek Grafana)
4. Manual approval → deploy ke production (GitHub Environments)
5. Rollback: `helm rollback idx-stream` atau revert terraform
```

### I. GitHub Environments (Settings → Environments)

Buat 3 environments di GitHub repo:
- `staging`: required reviewers = 0, auto-deploy on push to main
- `production`: required reviewers = 1 (mandatory approval), deployment branch = main

---

## Acceptance Criteria

- [ ] Pipeline bisa dijalankan di 3 environment berbeda tanpa modifikasi kode
- [ ] Environment variable menentukan env apa yang aktif
- [ ] Terraform workspace terpisah untuk setiap environment
- [ ] dbt target auto-switch berdasarkan env
- [ ] Production deployment membutuhkan manual approval
- [ ] `.env.prod` dan `.env.staging` tidak ada di repo (gitignored)
- [ ] Resource limits disesuaikan otomatis per environment
