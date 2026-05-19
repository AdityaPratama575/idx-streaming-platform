# ARCHITECTURE DOCUMENTATION: IDX-STREAM ANALYTICS ENGINE

## 1. Project Overview
**Name:** IDX-Stream: Real-Time Sectoral Analytics Engine
**Repository:** `git@github.com:AdityaPratama575/idx-streaming-platform.git`
**Objective:** Build a hybrid real-time data pipeline to monitor the Top 5 stocks from each business sector in the Indonesia Stock Exchange (IDX) using a streaming architecture.

## 2. Technical Stack
- **Data Source:** Python `yfinance` library (tickers suffixed with `.JK`).
- **Orchestration & Infrastructure:** Docker Compose (running on WSL2).
- **Message Broker:** Apache Kafka (KRaft Mode).
- **Stream Processing:** Apache Spark (Structured Streaming).
- **Cloud Data Warehouse:** Google BigQuery (Sink).
- **Transformation Layer:** dbt (data build tool) running on BigQuery.
- **Configuration Management:** `.env` for all environmental variables.

## 3. Data Architecture Detail

### A. Producer Layer (Ingestion)
- **Component:** Python Producer.
- **Task:** Fetch intraday stock data (Price, Volume, High, Low) for ~50+ tickers (Top 5 per sector).
- **Interval:** Controlled by `FETCH_INTERVAL_SECONDS` in `.env`.
- **Output:** JSON payload sent to Kafka topic `idx_sector_ticks`.

### B. Messaging Layer (Buffering)
- **Component:** Apache Kafka (single node, KRaft mode).
- **Internal Port:** `29092` (for Spark-to-Kafka communication).
- **External Port:** `9092` (for Producer-to-Kafka communication).
- **Role:** Decoupling the ingestion and processing layers to ensure fault tolerance.

### C. Processing Layer (Transformation)
- **Component:** Apache Spark Structured Streaming.
- **Operations:**
    - Read from Kafka topic.
    - Schema enforcement (JSON to Struct).
    - Data Cleaning (Handling NaNs and Type conversion).
    - Windowing/Aggregation (Optional: Moving averages).
    - Checkpointing: Uses `SPARK_CHECKPOINT_DIR` to ensure exactly-once semantics.
- **Connector:** `spark-bigquery-with-dependencies`.

### D. Storage Layer (Sink)
- **Component:** Google BigQuery.
- **Dataset:** Defined in `.env` (`GCP_BIGQUERY_DATASET`).
- **Table:** Defined in `.env` (`GCP_BIGQUERY_TABLE`).
- **Mode:** Append mode for historical analysis.

## 4. Infrastructure Diagram (Conceptual)
`[Producer] --(JSON/9092)--> [Kafka] --(Stream/29092)--> [Spark] --(BigQuery Connector)--> [GCP BigQuery]`

## 5. Directory Structure
```text
idx-streaming-platform/
├── .env                          # Secrets (gitignored)
├── .env.example                  # Template konfigurasi
├── .gitignore                    # Security rules
├── .dockerignore                 # Build context filter
├── gcp-service-account.json      # Credentials GCP (gitignored)
├── docker-compose.yml            # Orkestrasi 5 service
├── Dockerfile.producer           # Image untuk producer
├── Dockerfile.spark              # Image untuk spark processor
├── requirements-producer.txt     # Dependencies producer
├── requirements-spark.txt        # Dependencies spark
├── producer.py                   # Python: yfinance → Kafka
├── spark_processor.py            # Spark: Kafka → BigQuery
├── top5_saham_ihsg_by_sector_market_cap.py  # Daftar ticker per sektor
├── README.md
├── docs/
│   ├── agents.md                 # Aturan operasional agent
│   └── instruction.md            # Dokumentasi arsitektur
└── issue/
    ├── 01-docker-compose.md
    ├── 02-dockerfiles-dan-dependencies.md
    ├── 03-producer.md
    ├── 04-spark-processor.md
    ├── 05-readme.md
    ├── 06-spark-processor-bugfixes.md
    ├── 07-producer-edge-cases.md
    ├── 08-infrastructure-gaps.md
    └── 09-documentation-operational.md