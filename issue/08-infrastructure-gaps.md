# Issue 08: Infrastructure & Configuration Gaps

## Tujuan
Memperbaiki celah keamanan, konfigurasi, dan reliability pada layer Docker/infrastructure yang tidak tertangkap di Issue 01 dan 02.

---

## Gap 1: Tidak Ada `.dockerignore` — Build Context Bloat & Secret Leak Risk

### Lokasi
Root project — file `.dockerignore` tidak ada.

### Masalah
Tanpa `.dockerignore`, Docker build context mengirim **seluruh isi direktori** ke Docker daemon. Jika `.env` belum di-gitignore atau ada file sensitif lain, ini bisa:
- Membocorkan secret ke dalam Docker image layer
- Memperlambat build karena mengirim file yang tidak relevan (`__pycache__/`, `*.log`, `data/`, `docs/`, `issue/`)

### Perbaikan
Buat `.dockerignore` dengan konten:
```dockerignore
# Secrets & Config (seharusnya sudah di .gitignore, tapi safeguard lapis kedua)
.env
gcp-service-account.json
*.json

# Python
__pycache__/
*.py[cod]
.venv/
env/
venv/

# Data & Logs
*.log
data/
temp/
checkpoints/
spark-checkpoints/

# IDE
.vscode/
.idea/

# OS
.DS_Store
Thumbs.db

# Git
.git/
.gitignore
.gitattributes

# Docs & Issues (tidak dibutuhkan di image)
docs/
issue/
README.md
```

---

## Gap 2: `.gitignore` `*.json` Terlalu Broad

### Lokasi
`.gitignore` baris 5:
```gitignore
# Mengunci file kunci GCP agar tidak terendus bot scanner di GitHub
gcp-service-account.json
*.json
```

### Masalah
Rule `*.json` memblokir **semua file JSON**, termasuk:
- File konfigurasi yang legitimate (misal `tsconfig.json`, `package.json`, dll di masa depan)
- File data yang perlu dilacak di repo

Rule `gcp-service-account.json` sudah spesifik dan cukup. `*.json` adalah **safeguard berlebihan** yang akan menyulitkan pengembangan.

### Perbaikan
Hapus baris `*.json`:
```gitignore
# === Secrets & Configurations ===
.env
# Mengunci file kunci GCP agar tidak terendus bot scanner di GitHub
gcp-service-account.json
```
Atau ganti dengan pola yang lebih sempit:
```gitignore
*-service-account.json
*.credentials.json
```

---

## Gap 3: `spark-master` dan `spark-worker` Tidak Punya `restart` Policy

### Lokasi
`docker-compose.yml` baris 63-98 — service `spark-master` dan `spark-worker`.

### Masalah
Service `producer` dan `spark-processor` sudah memiliki `restart: on-failure`, tapi `spark-master` dan `spark-worker` tidak. Jika Spark master/worker crash (OOM, signal kill, dll), mereka **tidak akan restart otomatis**.

### Perbaikan
Tambahkan `restart: on-failure` ke kedua service:
```yaml
spark-master:
  # ... konfigurasi lain ...
  restart: on-failure

spark-worker:
  # ... konfigurasi lain ...
  restart: on-failure
```

---

## Gap 4: Spark `--packages` Download JARs Saat Runtime — Startup Lambat & Tidak Reliable

### Lokasi
`docker-compose.yml` baris 138-141 — command `spark-processor`:
```yaml
command: >
  spark-submit
  --master spark://spark-master:7077
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0
  /app/spark_processor.py
```

### Masalah
Flag `--packages` mengunduh JAR connector dari Maven Central **setiap kali container start**:
- Startup lambat (tergantung kecepatan network)
- Tidak reliable (Maven Central down → pipeline gagal start)
- Tidak konsisten (versi JAR bisa berubah jika tidak di-pin)

Issue 02 (`issue/02-dockerfiles-dan-dependencies.md`) sudah menyarankan pre-download JAR di Dockerfile, tapi belum diimplementasikan.

### Perbaikan
1. Download JAR di `Dockerfile.spark` saat build:
   ```dockerfile
   # Download Spark connectors
   RUN wget -P /opt/bitnami/spark/jars/ \
       https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
       wget -P /opt/bitnami/spark/jars/ \
       https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.34.0/spark-bigquery-with-dependencies_2.12-0.34.0.jar
   ```
2. Hapus `--packages` dari `docker-compose.yml` command (JAR sudah di classpath).

> Alternatif: gunakan `--jars` sebagai fallback jika ingin tetap menggunakan `--packages`.

---

## Gap 5: `.env.example` Memiliki Variable Tidak Terpakai (Dead Config)

### Lokasi
`.env.example` baris 5 dan 16:
```
GOOGLE_APPLICATION_CREDENTIALS="./gcp-service-account.json"
SPARK_MASTER="spark://spark-master:7077"
```

### Masalah
- `GOOGLE_APPLICATION_CREDENTIALS` di-spesifikkan di `.env.example` tapi path di-*hardcode* oleh `spark_processor.py:21` (lihat Issue 06, Bug 1). Variable ini **ada tapi tidak dipakai**.
- `SPARK_MASTER` didefinisikan tapi tidak digunakan oleh kode Python manapun. Master URL di-set via `--master` di docker-compose command.

Dead config membingungkan developer dan bisa menyebabkan asumsi salah ("saya ganti di .env, kok tidak berpengaruh?").

### Perbaikan
1. Setelah Issue 06 (Bug 1) diperbaiki, `GOOGLE_APPLICATION_CREDENTIALS` akan benar-benar digunakan — pertahankan di `.env.example` dan pastikan path default sesuai mount point container.
2. Hapus `SPARK_MASTER` dari `.env.example` karena tidak digunakan oleh kode (atau jika ingin tetap ada, beri komentar bahwa variabel ini tidak dipakai Python code, hanya sebagai referensi).

---

## Gap 6: `KAFKA_AUTO_CREATE_TOPICS_ENABLE: true` vs Spec `false`

### Lokasi
`docker-compose.yml` baris 50 vs `issue/01-docker-compose.md` baris 34.

### Masalah
Spec Issue 01 mensyaratkan `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` (production-ready), tapi implementasi menggunakan `true` (development convenience). Inkonsistensi ini harus diselesaikan:
- Jika diputuskan tetap `true`, beri komentar jelas bahwa ini **WAJIB diubah ke `false`** sebelum deployment production
- Jika diubah ke `false`, tambahkan step di README untuk membuat topic secara manual

### Perbaikan
1. Ubah ke `false` dan dokumentasikan cara membuat topic manual:
   ```bash
   docker exec idx-kafka kafka-topics --bootstrap-server localhost:29092 \
     --create --topic idx_sector_ticks --partitions 3 --replication-factor 1
   ```
2. Atau buat init container / startup script yang auto-create topic dengan konfigurasi yang benar.

---

## Acceptance Criteria

- [ ] `.dockerignore` ada dan mencakup secret, Python cache, docs, dan file tidak relevan
- [ ] `.gitignore` tidak memblokir semua file JSON (hanya credential-specific)
- [ ] `spark-master` dan `spark-worker` restart otomatis setelah crash
- [ ] JAR connector Spark di-download saat `docker build`, bukan saat `docker run`
- [ ] `.env.example` tidak mengandung variable yang tidak terpakai
- [ ] `KAFKA_AUTO_CREATE_TOPICS_ENABLE` konsisten antara spec dan implementasi
- [ ] `docker-compose up --build` tetap berjalan sukses tanpa regresi
