import json
import logging
import os
import time
from datetime import datetime, timezone

import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from kafka import KafkaProducer

# Load konfigurasi dari .env — semua path/host dibaca dari sini (BAN 1 compliance)
load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
FETCH_INTERVAL_SECONDS = int(os.getenv("FETCH_INTERVAL_SECONDS", 60))
YFINANCE_DELAY_SECONDS = float(os.getenv("YFINANCE_DELAY_SECONDS", 0.5))

from top5_saham_ihsg_by_sector_market_cap import top5_saham_ihsg_by_sector_market_cap

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def init_kafka_producer():
    created = False
    producer = None
    # Retry koneksi ke Kafka sampai berhasil, karena Kafka mungkin belum siap saat container start
    while not created:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                # Idempotent producer menjamin exactly-once delivery meski ada retry
                # enable_idempotence=True meng-override acks, retries, dan retry_backoff_ms
                enable_idempotence=True,
            )
            # Verifikasi koneksi dengan bootstrap_connected()
            if producer.bootstrap_connected():
                created = True
                logger.info("Kafka producer connected to %s", KAFKA_BOOTSTRAP_SERVERS)
        except Exception as e:
            logger.error("Kafka not ready yet: %s. Retrying in 5s...", e)
            time.sleep(5)
    return producer


def _format_ts(ts):
    if hasattr(ts, "tz_localize") and hasattr(ts, "tzinfo") and ts.tzinfo is None:
        ts = ts.tz_localize("Asia/Jakarta").tz_convert("UTC")
    if hasattr(ts, "isoformat"):
        s = ts.isoformat()
        # Hapus microseconds agar format konsisten untuk Spark
        if "." in s:
            s = s.split(".")[0] + s[s.index("+"):]
        return s
    return str(ts)


def build_payload(ticker, sector, row):
    return {
        "ticker": ticker,
        "sector": sector,
        # Timestamp dari data yfinance — dinormalisasi ke UTC agar konsisten
        "timestamp": _format_ts(row.name),
        # Timestamp kapan data di-fetch oleh sistem ini
        "fetch_ts": _format_ts(datetime.now(timezone.utc)),
        "open": float(row["Open"]) if pd.notna(row["Open"]) else None,
        "high": float(row["High"]) if pd.notna(row["High"]) else None,
        "low": float(row["Low"]) if pd.notna(row["Low"]) else None,
        "close": float(row["Close"]) if pd.notna(row["Close"]) else None,
        "volume": int(row["Volume"]) if pd.notna(row["Volume"]) else None,
    }


def fetch_and_publish(producer):
    max_retries = 3
    total_sent = 0
    for sector, tickers in top5_saham_ihsg_by_sector_market_cap.items():
        for ticker in tickers:
            for attempt in range(max_retries):
                try:
                    # Ambil data intraday 1 hari dengan interval 1 menit
                    data = yf.Ticker(ticker).history(period="1d", interval="1m")

                    if data.empty:
                        logger.warning("No data for %s (%s) — skipping", ticker, sector)
                        break

                    # Kirim setiap baris data sebagai satu message Kafka
                    for _, row in data.iterrows():
                        payload = build_payload(ticker, sector, row)
                        producer.send(KAFKA_TOPIC, value=payload)
                        total_sent += 1

                    logger.info("%s (%s): %d ticks sent", ticker, sector, len(data))
                    break

                except Exception as e:
                    if attempt < max_retries - 1:
                        wait = 5 * (attempt + 1)
                        logger.warning(
                            "Retry %d/%d for %s (%s) in %ds: %s",
                            attempt + 1, max_retries, ticker, sector, wait, e,
                        )
                        time.sleep(wait)
                    else:
                        # Retries habis — skip ticker, pipeline tetap jalan
                        logger.warning(
                            "Failed to fetch %s (%s) after %d retries: %s",
                            ticker, sector, max_retries, e,
                        )

            # Delay antar ticker untuk menghindari rate limit Yahoo Finance
            time.sleep(YFINANCE_DELAY_SECONDS)

        # Flush per sektor agar data tidak menumpuk di buffer dan latency terkontrol
        producer.flush()

    logger.info("Batch complete: %d total messages sent", total_sent)


def main():
    logger.info("Starting IDX Producer...")
    producer = init_kafka_producer()

    while True:
        try:
            fetch_and_publish(producer)
            logger.info("Sleeping %d seconds before next fetch cycle...", FETCH_INTERVAL_SECONDS)
            time.sleep(FETCH_INTERVAL_SECONDS)
        except Exception as e:
            # Kafka connection loss — reconnect dan langsung loop lagi (tanpa sleep tambahan)
            logger.error("Producer error: %s. Reconnecting...", e)
            time.sleep(5)
            producer = init_kafka_producer()


if __name__ == "__main__":
    main()
