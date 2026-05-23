import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from kafka import KafkaProducer

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

WIB = timezone(timedelta(hours=7))
_market_open = 9 * 60
_market_close = 15 * 60

_latest_ts_per_ticker: dict[str, str] = {}


def init_kafka_producer():
    created = False
    producer = None
    while not created:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                enable_idempotence=True,
            )
            if producer.bootstrap_connected():
                created = True
                logger.info("Kafka producer connected to %s", KAFKA_BOOTSTRAP_SERVERS)
        except Exception as e:
            logger.error("Kafka not ready yet: %s. Retrying in 5s...", e)
            time.sleep(5)
    return producer


def _is_market_open():
    now = datetime.now(WIB)
    current_minutes = now.hour * 60 + now.minute
    return now.weekday() < 5 and _market_open <= current_minutes < _market_close


def _get_sleep_interval():
    return FETCH_INTERVAL_SECONDS if _is_market_open() else 3600


def _format_ts(ts):
    if hasattr(ts, "tz_localize") and hasattr(ts, "tzinfo") and ts.tzinfo is None:
        ts = ts.tz_localize("Asia/Jakarta")
    if hasattr(ts, "isoformat"):
        s = ts.isoformat()
        if "." in s:
            s = s.split(".")[0] + s[s.index("+"):]
        return s
    return str(ts)


def build_payload(ticker, sector, row):
    return {
        "ticker": ticker,
        "sector": sector,
        "timestamp": _format_ts(row.name),
        "fetch_ts": _format_ts(datetime.now(WIB)),
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
                    data = yf.Ticker(ticker).history(period="1d", interval="1m")

                    if data.empty:
                        logger.warning("No data for %s (%s) — skipping", ticker, sector)
                        break

                    sent = 0
                    skipped = 0
                    latest = _latest_ts_per_ticker.get(ticker, "")

                    for _, row in data.iterrows():
                        ts = _format_ts(row.name)
                        if ts <= latest:
                            skipped += 1
                            continue
                        payload = build_payload(ticker, sector, row)
                        producer.send(
                            KAFKA_TOPIC,
                            value=payload,
                            headers=[
                                ("schema_name", b"stock_tick"),
                                ("schema_version", b"1"),
                            ],
                        )
                        sent += 1
                        if ts > latest:
                            latest = ts

                    if sent > 0:
                        _latest_ts_per_ticker[ticker] = latest
                        total_sent += sent
                        logger.info("%s (%s): sent=%d skipped=%d", ticker, sector, sent, skipped)
                    else:
                        logger.info("%s (%s): no new candles (skipped=%d)", ticker, sector, skipped)
                    break

                except Exception as e:
                    if attempt < max_retries - 1:
                        wait = 5 * (attempt + 1)
                        logger.warning("Retry %d/%d for %s (%s) in %ds: %s", attempt + 1, max_retries, ticker, sector, wait, e)
                        time.sleep(wait)
                    else:
                        logger.warning("Failed to fetch %s (%s) after %d retries: %s", ticker, sector, max_retries, e)

            time.sleep(YFINANCE_DELAY_SECONDS)

        producer.flush()

    logger.info("Batch complete: %d new messages sent across %d tickers with data", total_sent, len(_latest_ts_per_ticker))


def main():
    logger.info("Starting IDX Producer...")
    producer = init_kafka_producer()

    while True:
        try:
            fetch_and_publish(producer)
            interval = _get_sleep_interval()
            status = "market hours" if _is_market_open() else "off-hours"
            logger.info("Sleeping %d seconds before next fetch cycle (%s)...", interval, status)
            time.sleep(interval)
        except Exception as e:
            logger.error("Producer error: %s. Reconnecting...", e)
            time.sleep(5)
            producer = init_kafka_producer()


if __name__ == "__main__":
    main()
