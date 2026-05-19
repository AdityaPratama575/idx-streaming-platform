# Issue 07: producer.py — Resilience & Performance Gaps

## Tujuan
Meningkatkan ketahanan dan performa `producer.py` terhadap edge case yang muncul di production: rate limiting Yahoo Finance, exactly-once delivery guarantee, dan reconnect behavior.

---

## Gap 1: Rate Limiting Yahoo Finance — 55 Request Tanpa Delay

### Lokasi
`producer.py` baris 68-91 — fungsi `fetch_and_publish()`:
```python
for sector, tickers in top5_saham_ihsg_by_sector_market_cap.items():
    for ticker in tickers:
        # ... yf.Ticker(ticker).history(period="1d", interval="1m")
```

### Masalah
Loop melakukan 55 request ke Yahoo Finance secara **sequential tanpa jeda**. Yahoo Finance API memiliki rate limit tidak resmi (sekitar 2000 request/jam, tapi bisa throttle kapan saja). 55 rapid request bisa memicu temporary ban (HTTP 429) atau data kosong. Saat ini kode hanya `continue` ke ticker berikutnya — data hilang tanpa retry.

### Perbaikan
1. Tambahkan delay kecil (`0.5-1.0` detik) antar request ticker, bisa di-config via `.env`:
   ```
   YFINANCE_DELAY_SECONDS=0.5
   ```
2. Retry terpisah untuk HTTP 429 / rate limit error (bukan langsung skip):
   ```python
   max_retries = 3
   for attempt in range(max_retries):
       try:
           data = yf.Ticker(ticker).history(period="1d", interval="1m")
           break
       except RateLimitError:  # atau exception pattern dari yfinance
           time.sleep(5 * (attempt + 1))  # exponential backoff
   ```

---

## Gap 2: Tidak Ada Idempotent Producer — Duplikasi Saat Reconnect

### Lokasi
`producer.py` baris 35-41 — `KafkaProducer()` constructor:
```python
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=3,
    retry_backoff_ms=1000,
)
```

### Masalah
Konfigurasi saat ini **tidak mengaktifkan idempotent producer**. Dengan Kafka, skenario ini bisa terjadi:
1. Producer kirim message batch — Kafka menerima tapi ACK gagal terkirim (network glitch)
2. Producer retry message yang sama
3. Kafka menyimpan message **duplikat**

Saat ini `acks='all'` + `retries=3` mengurangi risiko, tapi tidak menjamin *exactly-once* di sisi producer.

### Perbaikan
Aktifkan `enable_idempotence=True` (Kafka >= 0.11). Ini otomatis meng-set `acks='all'`, `max_in_flight_requests_per_connection=5`, dan `retries=Integer.MAX_VALUE`:
```python
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    enable_idempotence=True,
)
```

> **Catatan**: Idempotent producer membutuhkan `max.in.flight.requests.per.connection <= 5`. Hapus `acks`, `retries`, dan `retry_backoff_ms` karena akan di-override oleh `enable_idempotence=True`.

---

## Gap 3: Double-Sleep di Reconnect Path

### Lokasi
`producer.py` baris 99-113 — fungsi `main()`:
```python
while True:
    try:
        fetch_and_publish(producer)
    except Exception as e:
        logger.error("Producer error: %s. Reconnecting...", e)
        time.sleep(5)              # <-- sleep 1
        producer = init_kafka_producer()

    logger.info("Sleeping %d seconds...", FETCH_INTERVAL_SECONDS)
    time.sleep(FETCH_INTERVAL_SECONDS)  # <-- sleep 2
```

### Masalah
Saat Kafka connection loss terdeteksi:
1. `fetch_and_publish()` gagal → trigger `except` block
2. Sleep 5 detik + reconnect
3. Lanjut ke bawah → sleep `FETCH_INTERVAL_SECONDS` (default 60 detik)

Total wait = **65 detik** tidak produktif setelah reconnect berhasil. Jika reconnect hanya butuh 1 detik, pipeline idle 64 detik sia-sia.

### Perbaikan
Setelah reconnect berhasil, langsung lanjut ke fetch berikutnya tanpa full interval sleep:
```python
while True:
    try:
        fetch_and_publish(producer)
        logger.info("Sleeping %d seconds...", FETCH_INTERVAL_SECONDS)
        time.sleep(FETCH_INTERVAL_SECONDS)
    except Exception as e:
        logger.error("Producer error: %s. Reconnecting...", e)
        time.sleep(5)
        producer = init_kafka_producer()
        # Langsung loop lagi — akan fetch ulang tanpa sleep tambahan
```

---

## Gap 4: `total_sent` Counter Tidak Reset Antar Batch (false positive)

### Lokasi
`producer.py` baris 69, 96 — fungsi `fetch_and_publish()`:
```python
def fetch_and_publish(producer):
    total_sent = 0       # <-- reset di sini
    # ...
    logger.info("Batch complete: %d total messages sent", total_sent)
```

### Analisis
Setelah inspeksi: counter sudah di-reset di awal setiap panggilan `fetch_and_publish()`, jadi ini **false positive**. Tidak perlu perbaikan. Namun untuk kejelasan, tambahkan log di awal batch:
```python
logger.info("Starting fetch cycle...")
```

---

## Gap 5: yfinance `history()` Bisa Return Timezone-Naive Timestamp

### Lokasi
`producer.py` baris 57:
```python
"timestamp": row.name.isoformat() if hasattr(row.name, "isoformat") else str(row.name),
```

### Masalah
`yf.Ticker(ticker).history()` bisa mengembalikan DatetimeIndex yang **timezone-naive** saat market tutup (non-trading hours). `.isoformat()` pada datetime naive menghasilkan `2026-05-06T00:00:00` tanpa timezone info — Spark processor di downstream mungkin gagal parse (lihat Issue 06, Bug 5).

### Perbaikan
1. Normalisasi timestamp ke UTC secara eksplisit sebelum `.isoformat()`:
   ```python
   ts = row.name
   if hasattr(ts, "tz_localize") and ts.tzinfo is None:
       ts = ts.tz_localize("Asia/Jakarta").tz_convert("UTC")
   "timestamp": ts.isoformat() if hasattr(ts, "isoformat") else str(ts),
   ```
2. Atau pastikan konsistensi format di sisi producer dan Spark (pilih salah satu, lalu sesuaikan parser di Spark).

---

## Acceptance Criteria

- [ ] Producer tidak kena rate limit Yahoo Finance (ada delay antar request)
- [ ] Producer menggunakan `enable_idempotence=True` untuk exactly-once delivery
- [ ] Reconnect path tidak double-sleep (langsung fetch ulang setelah reconnect)
- [ ] Timestamp selalu UTC-aware (tidak timezone-naive)
- [ ] Tidak ada regresi pada flow: yfinance → payload → Kafka send
