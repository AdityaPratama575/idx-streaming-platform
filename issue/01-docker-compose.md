# Issue 01: docker-compose.yml — Infrastruktur Orkestrasi

## Tujuan
Membuat file `docker-compose.yml` yang mendefinisikan seluruh infrastruktur pipeline IDX-Stream.

## Spesifikasi

### Services yang Harus Ada

| Service | Image | Ports | Keterangan |
|---|---|---|---|
| `kafka` | `confluentinc/cp-kafka:7.5.0` | `9092:9092`, `29092:29092` | KRaft mode, NO Zookeeper |
| `spark-master` | `bitnami/spark:3.5` | `8080:8080`, `7077:7077` | Cluster manager |
| `spark-worker` | `bitnami/spark:3.5` | `8081:8081` | Terhubung ke spark-master |
| `producer` | custom Dockerfile (lihat Issue 02) | - | depends_on kafka |
| `spark-processor` | custom Dockerfile (lihat Issue 02) | - | depends_on spark-master + kafka |

### Network
- Buat network `idx-network` dengan driver `bridge`.
- Semua service terhubung ke network ini.

### Konfigurasi Kafka (KRaft Mode)
Environment variables yang WAJIB:
```
KAFKA_NODE_ID=1
KAFKA_PROCESS_ROLES=broker,controller
KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093
KAFKA_LISTENERS=PLAINTEXT://kafka:29092,PLAINTEXT_HOST://0.0.0.0:9092,CONTROLLER://kafka:9093
KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT,CONTROLLER:PLAINTEXT
KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT
KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1
KAFKA_AUTO_CREATE_TOPICS_ENABLE=false
```
Pastikan listener `PLAINTEXT://kafka:29092` untuk komunikasi internal Docker.

### Konfigurasi Spark Master
```
SPARK_MODE=master
SPARK_MASTER_PORT=7077
SPARK_MASTER_WEBUI_PORT=8080
```

### Konfigurasi Spark Worker
```
SPARK_MODE=worker
SPARK_MASTER_URL=spark://spark-master:7077
SPARK_WORKER_WEBUI_PORT=8081
```

### Volumes
- `kafka_data:/var/lib/kafka/data` — persistensi data Kafka
- `spark_checkpoints:/tmp/spark-checkpoints` — checkpoint streaming
- Volume bind-mount `./gcp-service-account.json:/app/gcp-service-account.json:ro` untuk service spark-processor

### Producer Service
- build context dari Dockerfile (lihat Issue 02)
- env_file: `.env`
- depends_on: kafka (condition: service_healthy)
- restart: on-failure

### Spark-Processor Service
- build context dari Dockerfile (lihat Issue 02)
- env_file: `.env`
- depends_on: spark-master + kafka
- restart: on-failure
- command: jalankan `spark_processor.py` menggunakan `spark-submit` dengan `--master spark://spark-master:7077`

### Healthcheck (opsional tapi disarankan)
- Kafka: cek broker ready
- Spark master: cek port 8080

## Acceptance Criteria
- [ ] `docker-compose up` berjalan tanpa error
- [ ] `docker ps` menampilkan semua 5 service dalam status healthy/running
- [ ] Tidak ada service yang menggunakan `localhost` untuk komunikasi internal
- [ ] Kafka berjalan di KRaft mode (tidak ada Zookeeper container)
