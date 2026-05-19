# Issue 03: producer.py — Data Ingestion Layer

## Tujuan
Membuat Python script yang mengambil data saham dari yfinance dan mengirim ke Kafka.

## Spesifikasi

### Konfigurasi (dari `.env`)
- `KAFKA_BOOTSTRAP_SERVERS` — alamat broker Kafka (internal Docker: `kafka:29092`)
- `KAFKA_TOPIC` — nama topic (`idx_sector_ticks`)
- `FETCH_INTERVAL_SECONDS` — interval fetch

### Sumber Ticker
Import dari `top5_saham_ihsg_by_sector_market_cap.py`:
```python
from top5_saham_ihsg_by_sector_market_cap import top5_saham_ihsg_by_sector_market_cap
```

### Alur Program

```
1. Load .env menggunakan python-dotenv
2. Inisialisasi KafkaProducer dengan:
   - bootstrap_servers dari env
   - value_serializer: JSON encode + UTF-8
   - acks='all' untuk durability
   - Tambahkan config retries/retry_backoff_ms
3. LOOP utama (while True, sleep FETCH_INTERVAL_SECONDS):
   a. Iterasi dictionary ticker per sektor
   b. Untuk setiap ticker, panggil yfinance:
      - yf.Ticker(ticker).history(period="1d", interval="1m")
      - Atau yf.download() batch
   c. Untuk setiap row hasil fetch, buat payload JSON:
      {
        "ticker": "BBCA.JK",
        "sector": "Financials",
        "timestamp": ISO 8601 UTC,
        "fetch_ts": ISO 8601 UTC (kapan data di-fetch),
        "open": float,
        "high": float,
        "low": float,
        "close": float,
        "volume": int
      }
   d. Kirim ke Kafka: producer.send(topic, value=payload)
   e. producer.flush() setiap selesai 1 batch sektor
4. ERROR HANDLING:
   - yfinance error → log warning, lanjut ke ticker berikutnya
   - Kafka error → log error, sleep 5 detik, reconnect
   - Semua exception → try-except di outer loop
```

### Format Payload JSON (harus konsisten, akan diparse Spark)

```json
{
  "ticker": "BBCA.JK",
  "sector": "Financials",
  "timestamp": "2026-05-05T10:30:00+07:00",
  "fetch_ts": "2026-05-05T10:30:02+07:00",
  "open": 10050.0,
  "high": 10100.0,
  "low": 10025.0,
  "close": 10075.0,
  "volume": 1250000
}
```

### Logging
- Gunakan Python `logging` module
- Log level INFO: ticker yang sedang diproses, jumlah message terkirim
- Log level WARNING: API failure untuk ticker tertentu
- Log level ERROR: Kafka connection failure

## Acceptance Criteria
- [ ] Script berjalan tanpa crash saat ada ticker yang gagal di-fetch
- [ ] Payload JSON mengikuti schema di atas
- [ ] Tidak ada hardcoded credential atau path
- [ ] Semua config dibaca dari `.env`
- [ ] Producer reconnect otomatis jika Kafka restart
