import json
import time
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from producer import (
    _format_ts,
    _is_market_open,
    _latest_ts_per_ticker,
    build_payload,
    fetch_and_publish,
    init_kafka_producer,
)


class TestFormatTs:
    def test_timezone_naive(self):
        ts = pd.Timestamp("2026-05-21 09:30:00")
        result = _format_ts(ts)
        assert "+07:00" in result
        assert result.startswith("2026-05-21T09:30:00")

    def test_timezone_aware_wib(self):
        from datetime import timezone, timedelta
        ts = pd.Timestamp("2026-05-21 09:30:00", tz=timezone(timedelta(hours=7)))
        result = _format_ts(ts)
        assert "+07:00" in result

    def test_strips_microseconds(self):
        import pytz
        ts = pd.Timestamp("2026-05-21 09:30:00.123456", tz=pytz.UTC)
        result = _format_ts(ts)
        assert ".123456" not in result

    def test_non_timestamp_object(self):
        result = _format_ts("not-a-timestamp")
        assert result == "not-a-timestamp"


class TestBuildPayload:
    def test_valid_row(self, sample_row):
        payload = build_payload("BYAN.JK", "Energy", sample_row)
        assert payload["ticker"] == "BYAN.JK"
        assert payload["sector"] == "Energy"
        assert payload["open"] == 10000.0
        assert payload["high"] == 10100.0
        assert payload["low"] == 9900.0
        assert payload["close"] == 10050.0
        assert payload["volume"] == 1500000
        assert "timestamp" in payload
        assert "fetch_ts" in payload

    def test_nan_values(self, sample_row_with_nan):
        payload = build_payload("TEST.JK", "Test", sample_row_with_nan)
        assert payload["open"] is None
        assert payload["high"] is None
        assert payload["low"] is None
        assert payload["close"] is None
        assert payload["volume"] is None

    def test_all_fields_present(self, sample_row):
        payload = build_payload("BYAN.JK", "Energy", sample_row)
        expected_keys = {"ticker", "sector", "timestamp", "fetch_ts", "open", "high", "low", "close", "volume"}
        assert set(payload.keys()) == expected_keys


class TestInitKafkaProducer:
    @patch("producer.KafkaProducer")
    def test_retries_on_failure(self, mock_kafka):
        instance = MagicMock()
        instance.bootstrap_connected.side_effect = [False, False, True]
        mock_kafka.return_value = instance

        with patch("producer.time.sleep") as mock_sleep:
            result = init_kafka_producer()

        assert result == instance

    @patch("producer.KafkaProducer")
    def test_connected(self, mock_kafka):
        instance = MagicMock()
        instance.bootstrap_connected.return_value = True
        mock_kafka.return_value = instance

        result = init_kafka_producer()
        assert result.bootstrap_connected()


class TestFetchAndPublish:
    def test_success(self, mock_kafka_producer, mock_yfinance):
        from datetime import timezone, timedelta
        ts = pd.Timestamp("2026-05-21 09:30:00", tz=timezone(timedelta(hours=7)))
        data = pd.DataFrame({
            "Open": [10000.0],
            "High": [10100.0],
            "Low": [9900.0],
            "Close": [10050.0],
            "Volume": [1500000],
        })
        data.index = [ts]
        ticker = mock_yfinance.Ticker.return_value
        ticker.history.return_value = data

        with patch("producer.top5_saham_ihsg_by_sector_market_cap", {"Energy": ["BYAN.JK"]}):
            with patch("producer.time.sleep"):
                _latest_ts_per_ticker.clear()
                fetch_and_publish(mock_kafka_producer)

        assert mock_kafka_producer.send.called

    def test_empty_data(self, mock_kafka_producer, mock_yfinance):
        ticker = mock_yfinance.Ticker.return_value
        ticker.history.return_value = pd.DataFrame()

        with patch("producer.top5_saham_ihsg_by_sector_market_cap", {"Energy": ["BYAN.JK"]}):
            with patch("producer.time.sleep"):
                _latest_ts_per_ticker.clear()
                fetch_and_publish(mock_kafka_producer)

        mock_kafka_producer.send.assert_not_called()

    def test_retry_then_success(self, mock_kafka_producer, mock_yfinance):
        from datetime import timezone, timedelta
        ts = pd.Timestamp("2026-05-21 09:30:00", tz=timezone(timedelta(hours=7)))
        success_df = pd.DataFrame({
            "Open": [10000.0], "High": [10100.0], "Low": [9900.0],
            "Close": [10050.0], "Volume": [1500000],
        })
        success_df.index = [ts]

        ticker = mock_yfinance.Ticker.return_value
        ticker.history.side_effect = [Exception("fail"), Exception("fail"), success_df]

        with patch("producer.top5_saham_ihsg_by_sector_market_cap", {"Energy": ["BYAN.JK"]}):
            with patch("producer.time.sleep") as mock_sleep:
                _latest_ts_per_ticker.clear()
                fetch_and_publish(mock_kafka_producer)

        assert ticker.history.call_count == 3
        assert mock_kafka_producer.send.called

    def test_dedup_skips_duplicates(self, mock_kafka_producer, mock_yfinance):
        from datetime import timezone, timedelta
        ts = pd.Timestamp("2026-05-21 09:30:00", tz=timezone(timedelta(hours=7)))
        data = pd.DataFrame({
            "Open": [10000.0], "High": [10100.0], "Low": [9900.0],
            "Close": [10050.0], "Volume": [1500000],
        })
        data.index = [ts]
        ticker = mock_yfinance.Ticker.return_value
        ticker.history.return_value = data

        with patch("producer.top5_saham_ihsg_by_sector_market_cap", {"Energy": ["BYAN.JK"]}):
            with patch("producer.time.sleep"):
                _latest_ts_per_ticker.clear()
                fetch_and_publish(mock_kafka_producer)
                initial_send_count = mock_kafka_producer.send.call_count
                fetch_and_publish(mock_kafka_producer)

        assert mock_kafka_producer.send.call_count == initial_send_count


class TestIsMarketOpen:
    def test_returns_bool(self):
        result = _is_market_open()
        assert isinstance(result, bool)
