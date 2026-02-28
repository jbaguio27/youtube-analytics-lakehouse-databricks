from __future__ import annotations

from argparse import Namespace
from datetime import date, timedelta
import sys
import types

import pytest

# Minimal stub so unit tests can import the ingestion module without pyspark installed.
if "pyspark" not in sys.modules:
    pyspark_mod = types.ModuleType("pyspark")
    sql_mod = types.ModuleType("pyspark.sql")
    types_mod = types.ModuleType("pyspark.sql.types")

    class _Dummy:
        def __init__(self, *args, **kwargs) -> None:
            pass

    sql_mod.SparkSession = _Dummy
    types_mod.DateType = _Dummy
    types_mod.StringType = _Dummy
    types_mod.StructField = _Dummy
    types_mod.StructType = _Dummy
    types_mod.TimestampType = _Dummy

    pyspark_mod.sql = sql_mod
    sys.modules["pyspark"] = pyspark_mod
    sys.modules["pyspark.sql"] = sql_mod
    sys.modules["pyspark.sql.types"] = types_mod

from ingestion.tasks.ingest_analytics_api_to_bronze import _resolve_window


def _args(*, start_date: str, end_date: str, lookback_days: int = 7) -> Namespace:
    return Namespace(
        start_date=start_date,
        end_date=end_date,
        lookback_days=lookback_days,
    )


def test_resolve_window_accepts_auto_start_and_end() -> None:
    start, end, mode = _resolve_window(_args(start_date="auto", end_date="auto", lookback_days=7))
    assert mode == "rolling_lookback"
    assert end == date.today() - timedelta(days=1)
    assert start == end - timedelta(days=6)


def test_resolve_window_explicit_start_with_auto_end() -> None:
    start, end, mode = _resolve_window(_args(start_date="2025-01-01", end_date="auto"))
    assert mode == "explicit_date_range"
    assert start.isoformat() == "2025-01-01"
    assert end == date.today() - timedelta(days=1)


def test_resolve_window_rejects_invalid_start_date() -> None:
    with pytest.raises(ValueError, match="start-date must be in YYYY-MM-DD format"):
        _resolve_window(_args(start_date="autoo", end_date=""))


def test_resolve_window_rejects_start_after_end() -> None:
    with pytest.raises(ValueError, match="start-date must be on or before end-date"):
        _resolve_window(_args(start_date="2025-02-01", end_date="2025-01-31"))
