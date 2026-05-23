import json
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


@pytest.fixture
def sample_payload():
    with open(os.path.join(FIXTURES_DIR, "sample_payload.json")) as f:
        return json.load(f)


@pytest.fixture
def mock_kafka_producer():
    with patch("producer.KafkaProducer") as mock:
        instance = MagicMock()
        instance.bootstrap_connected.return_value = True
        mock.return_value = instance
        yield instance


@pytest.fixture
def mock_yfinance():
    with patch("producer.yf") as mock:
        yield mock


@pytest.fixture
def sample_row():
    import pandas as pd
    from datetime import timezone, timedelta

    ts = pd.Timestamp("2026-05-21 09:30:00", tz=timezone(timedelta(hours=7)))
    row = pd.Series({
        "Open": 10000.0,
        "High": 10100.0,
        "Low": 9900.0,
        "Close": 10050.0,
        "Volume": 1500000,
    })
    row.name = ts
    return row


@pytest.fixture
def sample_row_with_nan():
    import pandas as pd
    from datetime import timezone, timedelta

    ts = pd.Timestamp("2026-05-21 09:30:00", tz=timezone(timedelta(hours=7)))
    row = pd.Series({
        "Open": float("nan"),
        "High": float("nan"),
        "Low": float("nan"),
        "Close": float("nan"),
        "Volume": float("nan"),
    })
    row.name = ts
    return row
