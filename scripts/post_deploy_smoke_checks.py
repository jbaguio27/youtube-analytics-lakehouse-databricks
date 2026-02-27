"""Post-deploy smoke checks for Databricks lakehouse objects and data freshness."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

try:
    from pyspark.sql import SparkSession
except Exception:  # pragma: no cover - local execution without pyspark installed
    SparkSession = None  # type: ignore[assignment]

REQUIRED_TABLES: dict[str, list[str]] = {
    "bronze": [
        "run_context_log",
        "channels_raw",
        "videos_raw",
        "analytics_channel_daily_raw",
        "analytics_video_daily_raw",
    ],
    "silver_physical": [
        "silver_channels",
        "silver_videos",
        "fact_channel_daily_metrics",
        "fact_video_daily_metrics",
    ],
    "gold": [
        "gold_channel_daily_summary",
        "gold_video_daily_summary",
        "gold_video_country_daily_summary",
        "gold_video_device_daily_summary",
        "gold_video_traffic_source_daily_summary",
    ],
}


def _run_databricks(args: list[str]) -> subprocess.CompletedProcess[str]:
    cmd = ["databricks", *args]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"Databricks CLI command failed: {' '.join(cmd)}\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}"
        )
    return result


def _dbx_api(
    *,
    method: str,
    path: str,
    profile: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    args = ["api", method, path, "-o", "json"]
    if profile:
        args.extend(["--profile", profile])
    tmp_path: Path | None = None
    if payload is not None:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, encoding="utf-8"
        ) as f:
            json.dump(payload, f, separators=(",", ":"))
            tmp_path = Path(f.name)
        args.extend(["--json", f"@{tmp_path}"])

    try:
        result = _run_databricks(args)
        return json.loads(result.stdout)
    finally:
        if tmp_path and tmp_path.exists():
            tmp_path.unlink()


def _submit_sql_and_wait(
    *,
    warehouse_id: str,
    statement: str,
    profile: str,
    poll_seconds: int,
    timeout_seconds: int,
) -> dict[str, Any]:
    submit = _dbx_api(
        method="post",
        path="/api/2.0/sql/statements",
        profile=profile,
        payload={
            "warehouse_id": warehouse_id,
            "statement": statement,
            "wait_timeout": "10s",
        },
    )
    statement_id = submit.get("statement_id")
    if not statement_id:
        raise RuntimeError("Missing statement_id from SQL submit response.")

    elapsed = 0
    while elapsed <= timeout_seconds:
        status = _dbx_api(
            method="get",
            path=f"/api/2.0/sql/statements/{statement_id}",
            profile=profile,
        )
        state = status.get("status", {}).get("state")
        if state == "SUCCEEDED":
            return status
        if state in {"FAILED", "CANCELED", "CLOSED"}:
            msg = status.get("status", {}).get("error", {}).get("message", "No error message.")
            raise RuntimeError(f"Statement failed (id={statement_id}, state={state}): {msg}")
        time.sleep(poll_seconds)
        elapsed += poll_seconds

    raise TimeoutError(
        f"Statement timed out after {timeout_seconds}s (statement_id={statement_id})."
    )


def _rows_from_status(status: dict[str, Any]) -> list[list[Any]]:
    result = status.get("result", {})
    rows = result.get("data_array", [])
    return rows if isinstance(rows, list) else []


def _single_value(
    *,
    warehouse_id: str,
    profile: str,
    poll_seconds: int,
    timeout_seconds: int,
    query: str,
) -> Any:
    status = _submit_sql_and_wait(
        warehouse_id=warehouse_id,
        statement=query,
        profile=profile,
        poll_seconds=poll_seconds,
        timeout_seconds=timeout_seconds,
    )
    rows = _rows_from_status(status)
    if not rows:
        return None
    return rows[0][0]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run post-deploy smoke checks.")
    parser.add_argument(
        "--execution-mode",
        choices=["auto", "sql_warehouse", "spark"],
        default="auto",
        help="auto: spark in Databricks job, otherwise sql_warehouse via Databricks SQL API.",
    )
    parser.add_argument(
        "--warehouse-id",
        default=os.getenv("DATABRICKS_SQL_WAREHOUSE_ID", ""),
        help="Databricks SQL warehouse id (raw id only).",
    )
    parser.add_argument(
        "--profile",
        default=os.getenv("DATABRICKS_CONFIG_PROFILE", ""),
        help="Databricks CLI profile. Optional when using DATABRICKS_HOST/TOKEN env vars.",
    )
    parser.add_argument(
        "--catalog",
        default=os.getenv("YOUTUBE_ANALYTICS_CATALOG", "youtube_analytics_dev"),
        help="Catalog to validate.",
    )
    parser.add_argument("--poll-seconds", type=int, default=3)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument(
        "--max-gold-lag-days",
        type=int,
        default=7,
        help="Fail if latest Gold daily data is older than this many days.",
    )
    parser.add_argument(
        "--require-latest-run-success",
        choices=["true", "false"],
        default="true",
        help="When true, fail if latest run_status is not 'success'.",
    )
    return parser.parse_args()


def _require(name: str, value: str) -> str:
    if not value:
        raise ValueError(f"{name} is required. Provide arg or set it in .env.")
    return value


def _run_smoke_checks_sql_warehouse(
    *,
    warehouse_id: str,
    profile: str,
    catalog: str,
    poll_seconds: int,
    timeout_seconds: int,
    max_gold_lag_days: int,
    require_latest_run_success: bool,
) -> tuple[dict[str, Any], list[str]]:
    checks: dict[str, Any] = {}
    errors: list[str] = []

    existing: set[tuple[str, str]] = set()
    for schema, tables in REQUIRED_TABLES.items():
        for table in tables:
            show_query = f"SHOW TABLES IN {catalog}.{schema} LIKE '{table}'"
            show_status = _submit_sql_and_wait(
                warehouse_id=warehouse_id,
                statement=show_query,
                profile=profile,
                poll_seconds=poll_seconds,
                timeout_seconds=timeout_seconds,
            )
            show_rows = _rows_from_status(show_status)
            if show_rows:
                existing.add((schema, table))
    missing: list[str] = []
    for schema, tables in REQUIRED_TABLES.items():
        for table in tables:
            if (schema, table) not in existing:
                missing.append(f"{catalog}.{schema}.{table}")
    checks["required_objects"] = {
        "expected_count": sum(len(v) for v in REQUIRED_TABLES.values()),
        "found_count": len(existing),
        "missing": missing,
    }
    if missing:
        errors.extend([f"Missing object: {m}" for m in missing])

    latest_status_query = f"""
    SELECT run_status
    FROM {catalog}.bronze.run_context_log
    ORDER BY COALESCE(finalized_ts_utc, ingest_ts_utc) DESC
    LIMIT 1
    """
    latest_status = _single_value(
        warehouse_id=warehouse_id,
        profile=profile,
        poll_seconds=poll_seconds,
        timeout_seconds=timeout_seconds,
        query=latest_status_query,
    )
    checks["latest_run_status"] = latest_status
    if require_latest_run_success and str(latest_status or "").lower() != "success":
        errors.append(f"Latest run_status is not success: {latest_status!r}")

    core_gold_counts: dict[str, int] = {}
    for table in ["gold_channel_daily_summary", "gold_video_daily_summary"]:
        count_query = f"SELECT COUNT(*) FROM {catalog}.gold.{table}"
        count_value = _single_value(
            warehouse_id=warehouse_id,
            profile=profile,
            poll_seconds=poll_seconds,
            timeout_seconds=timeout_seconds,
            query=count_query,
        )
        count_int = int(count_value or 0)
        core_gold_counts[table] = count_int
        if count_int <= 0:
            errors.append(f"Core Gold table has no rows: {catalog}.gold.{table}")
    checks["core_gold_row_counts"] = core_gold_counts

    recency_query = f"""
    SELECT DATEDIFF(current_date(), max(date)) AS lag_days
    FROM {catalog}.gold.gold_video_daily_summary
    """
    lag_days_value = _single_value(
        warehouse_id=warehouse_id,
        profile=profile,
        poll_seconds=poll_seconds,
        timeout_seconds=timeout_seconds,
        query=recency_query,
    )
    lag_days = int(lag_days_value) if lag_days_value is not None else None
    checks["gold_recency_lag_days"] = lag_days
    checks["gold_recency_max_lag_days"] = max_gold_lag_days
    if lag_days is None:
        errors.append("Gold recency check returned NULL lag.")
    elif lag_days > max_gold_lag_days:
        errors.append(
            f"Gold data lag ({lag_days} days) exceeds threshold ({max_gold_lag_days} days)."
        )

    return checks, errors


def _run_smoke_checks_spark(
    *,
    catalog: str,
    max_gold_lag_days: int,
    require_latest_run_success: bool,
) -> tuple[dict[str, Any], list[str]]:
    if SparkSession is None:
        raise RuntimeError("PySpark is not available for spark execution mode.")

    spark = SparkSession.builder.getOrCreate()
    checks: dict[str, Any] = {}
    errors: list[str] = []

    existing: set[tuple[str, str]] = set()
    for schema, tables in REQUIRED_TABLES.items():
        for table in tables:
            if spark.catalog.tableExists(f"{catalog}.{schema}.{table}"):
                existing.add((schema, table))
    missing: list[str] = []
    for schema, tables in REQUIRED_TABLES.items():
        for table in tables:
            if (schema, table) not in existing:
                missing.append(f"{catalog}.{schema}.{table}")
    checks["required_objects"] = {
        "expected_count": sum(len(v) for v in REQUIRED_TABLES.values()),
        "found_count": len(existing),
        "missing": missing,
    }
    if missing:
        errors.extend([f"Missing object: {m}" for m in missing])

    latest_status_row = spark.sql(
        f"""
        SELECT run_status
        FROM {catalog}.bronze.run_context_log
        ORDER BY COALESCE(finalized_ts_utc, ingest_ts_utc) DESC
        LIMIT 1
        """
    ).first()
    latest_status = latest_status_row[0] if latest_status_row else None
    checks["latest_run_status"] = latest_status
    if require_latest_run_success and str(latest_status or "").lower() != "success":
        errors.append(f"Latest run_status is not success: {latest_status!r}")

    core_gold_counts: dict[str, int] = {}
    for table in ["gold_channel_daily_summary", "gold_video_daily_summary"]:
        count_row = spark.sql(f"SELECT COUNT(*) AS c FROM {catalog}.gold.{table}").first()
        count_int = int(count_row["c"] if count_row else 0)
        core_gold_counts[table] = count_int
        if count_int <= 0:
            errors.append(f"Core Gold table has no rows: {catalog}.gold.{table}")
    checks["core_gold_row_counts"] = core_gold_counts

    lag_row = spark.sql(
        f"SELECT DATEDIFF(current_date(), max(date)) AS lag_days FROM {catalog}.gold.gold_video_daily_summary"
    ).first()
    lag_days = int(lag_row["lag_days"]) if lag_row and lag_row["lag_days"] is not None else None
    checks["gold_recency_lag_days"] = lag_days
    checks["gold_recency_max_lag_days"] = max_gold_lag_days
    if lag_days is None:
        errors.append("Gold recency check returned NULL lag.")
    elif lag_days > max_gold_lag_days:
        errors.append(
            f"Gold data lag ({lag_days} days) exceeds threshold ({max_gold_lag_days} days)."
        )

    return checks, errors


def main() -> None:
    load_dotenv()
    args = _parse_args()
    mode = args.execution_mode
    require_latest_run_success = args.require_latest_run_success.lower() == "true"
    if mode == "auto":
        mode = "spark" if "dbutils" in globals() else "sql_warehouse"

    if mode == "spark":
        checks, errors = _run_smoke_checks_spark(
            catalog=args.catalog,
            max_gold_lag_days=args.max_gold_lag_days,
            require_latest_run_success=require_latest_run_success,
        )
    else:
        warehouse_id = _require("warehouse-id", args.warehouse_id)
        if "/" in warehouse_id:
            raise ValueError(
                "warehouse-id must be raw ID only (for example 6bdcbabcbe866b08), not a URL path."
            )
        checks, errors = _run_smoke_checks_sql_warehouse(
            warehouse_id=warehouse_id,
            profile=args.profile,
            catalog=args.catalog,
            poll_seconds=args.poll_seconds,
            timeout_seconds=args.timeout_seconds,
            max_gold_lag_days=args.max_gold_lag_days,
            require_latest_run_success=require_latest_run_success,
        )

    payload = {
        "status": "ok" if not errors else "failed",
        "execution_mode": mode,
        "catalog": args.catalog,
        "checks": checks,
        "errors": errors,
    }
    print(json.dumps(payload, indent=2))
    if errors:
        raise RuntimeError("Post-deploy smoke checks failed.")


if __name__ == "__main__":
    main()
