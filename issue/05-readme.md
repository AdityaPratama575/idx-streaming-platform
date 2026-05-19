# Issue 05: README.md — Project Documentation

## Tujuan
Membuat dokumentasi proyek yang jelas untuk developer yang akan menjalankan pipeline ini.

## Spesifikasi

### Struktur README.md

```markdown
# IDX-Stream: Real-Time Sectoral Analytics Engine

## Overview
[1-2 paragraf: apa proyek ini, tujuan, stack teknologi]

## Architecture
[Diagram sederhana: Producer → Kafka → Spark → BigQuery → dbt]

## Prerequisites
- Docker & Docker Compose
- Google Cloud Platform account + BigQuery enabled
- Service Account JSON key

## Setup

### 1. Clone & Configure
git clone ...
cp .env.example .env
# Edit .env dengan GCP Project ID dan konfigurasi lainnya

### 2. GCP Service Account
Letakkan gcp-service-account.json di root project.

### 3. Build & Run
docker-compose up --build

### 4. Verify
- Kafka UI (optional): localhost:9092
- Spark Master UI: http://localhost:8080
- BigQuery: cek dataset dan tabel

## Project Structure
[idx-streaming-platform/
├── .env
├── .env.example
├── .gitignore
├── gcp-service-account.json
├── docker-compose.yml
├── Dockerfile.producer
├── Dockerfile.spark
├── requirements-producer.txt
├── requirements-spark.txt
├── producer.py
├── spark_processor.py
├── top5_saham_ihsg_by_sector_market_cap.py
├── README.md
└── docs/
    ├── agents.md
    └── instruction.md]

## Configuration
[Table: environment variables dan deskripsinya]

## Troubleshooting
- Kafka tidak terkoneksi → cek network Docker
- BigQuery permission error → cek service account IAM
- Spark job stuck → cek checkpoint directory
```

## Acceptance Criteria
- [ ] Developer baru bisa setup dan run hanya dengan membaca README
- [ ] Ada daftar prerequisites yang jelas
- [ ] Ada troubleshooting section untuk error umum
- [ ] Diagram arsitektur jelas (bisa ASCII art)
