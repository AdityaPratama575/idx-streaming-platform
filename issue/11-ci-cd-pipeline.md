# Issue 11: CI/CD Pipeline — GitHub Actions

## Tujuan
Membuat automated CI/CD pipeline dengan GitHub Actions untuk linting, testing, build Docker image, dan push ke container registry.

---

## Spesifikasi

### A. Workflow: CI — Pull Request & Push to `main`

File: `.github/workflows/ci.yml`

**Trigger:** `push` ke `main`, `pull_request` ke `main`

**Jobs:**

#### Job 1: Lint

| Langkah | Detail |
|---|---|
| Checkout code | `actions/checkout@v4` |
| Setup Python 3.9 | `actions/setup-python@v5` |
| Install linter | `pip install ruff` |
| Run ruff check | `ruff check .` |
| Run ruff format | `ruff format --check .` |

#### Job 2: Test — Producer

| Langkah | Detail |
|---|---|
| Checkout code | `actions/checkout@v4` |
| Setup Python 3.9 | `actions/setup-python@v5` |
| Install deps | `pip install -r requirements-producer.txt -r requirements-test.txt` |
| Run pytest | `pytest tests/test_producer.py -v --cov=producer --cov-report=xml` |
| Upload coverage | `codecov/codecov-action@v4` (opsional) |

#### Job 3: Test — Spark Processor

| Langkah | Detail |
|---|---|
| Checkout code | `actions/checkout@v4` |
| Setup Java 11 | `actions/setup-java@v4` |
| Setup Python 3.9 | `actions/setup-python@v5` |
| Setup Spark | Download & extract Apache Spark 3.4.4 |
| Install deps | `pip install -r requirements-spark.txt -r requirements-test.txt` |
| Run pytest | `pytest tests/test_spark_processor.py -v --cov=spark_processor --cov-report=xml` |

### B. Workflow: CD — Build & Push Docker Images

File: `.github/workflows/cd.yml`

**Trigger:** Push tag `v*.*.*` atau manual dispatch (`workflow_dispatch`)

**Jobs:**

#### Job 1: Build & Push Producer Image

| Langkah | Detail |
|---|---|
| Checkout | `actions/checkout@v4` |
| Login to GHCR | `docker/login-action@v3` dengan `ghcr.io` |
| Metadata | `docker/metadata-action@v5` untuk tag otomatis |
| Build & Push | `docker/build-push-action@v6` — Dockerfile.producer, push ke `ghcr.io/adityapratama575/idx-producer` |

#### Job 2: Build & Push Spark Processor Image

| Langkah | Detail |
|---|---|
| Checkout | `actions/checkout@v4` |
| Login to GHCR | `docker/login-action@v3` dengan `ghcr.io` |
| Metadata | `docker/metadata-action@v5` |
| Build & Push | `docker/build-push-action@v6` — Dockerfile.spark, push ke `ghcr.io/adityapratama575/idx-spark-processor` |

### C. Workflow: Scheduled dbt Run (opsional, jika dbt sudah ada)

File: `.github/workflows/dbt-run.yml`

**Trigger:** cron `0 8 * * 1-5` (setiap hari kerja jam 8 pagi)

**Job:** Jalankan `dbt run` + `dbt test` via service account JSON dari GitHub Secrets.

### D. GitHub Secrets yang Dibutuhkan

| Secret | Deskripsi |
|---|---|
| `GCP_SERVICE_ACCOUNT_JSON` | Isi `gcp-service-account.json` untuk dbt job |
| `GHCR_TOKEN` | Token GitHub Container Registry (opsional, bisa pakai `GITHUB_TOKEN`) |

---

## Acceptance Criteria

- [ ] CI workflow berjalan otomatis saat push ke `main` dan PR
- [ ] Lint job memblokir merge jika ada pelanggaran
- [ ] Test job gagal jika ada test fail
- [ ] CD workflow push Docker image ke GHCR saat release tag
- [ ] Semua job selesai dalam < 10 menit
- [ ] Tidak ada secret atau credential yang bocor di log workflow
