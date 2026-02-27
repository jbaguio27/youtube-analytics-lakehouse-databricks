"""Step 2 orchestrator: Unity Catalog bootstrap + Bronze contract validation."""

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

DEFAULT_SQL_FILE = "lakeflow/bootstrap_unity_catalog.sql"

EXPECTED_TABLES = [
    "channels_raw",
    "playlist_items_raw",
    "videos_raw",
    "analytics_channel_daily_raw",
    "analytics_video_daily_raw",
    "analytics_video_traffic_source_daily_raw",
    "analytics_video_country_daily_raw",
    "analytics_video_device_daily_raw",
]

EXPECTED_METADATA = {
    "snapshot_date": {"data_type": "DATE", "is_nullable": "NO"},
    "ingest_ts_utc": {"data_type": "TIMESTAMP", "is_nullable": "NO"},
    "source_system": {"data_type": "STRING", "is_nullable": "NO"},
    "request_id": {"data_type": "STRING", "is_nullable": "NO"},
    "schema_version": {"data_type": "STRING", "is_nullable": "NO"},
    "run_id": {"data_type": "STRING", "is_nullable": "NO"},
    "payload": {"data_type": "STRING", "is_nullable": "NO"},
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
    args = ["api", method, path, "--profile", profile, "-o", "json"]
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


def _load_sql_statements(sql_file: Path) -> list[str]:
    if not sql_file.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_file}")
    lines = sql_file.read_text(encoding="utf-8").splitlines()
    body_lines = [line for line in lines if not line.strip().startswith("--")]
    sql = "\n".join(body_lines)
    return [stmt.strip() for stmt in sql.split(";") if stmt.strip()]


def _bootstrap_unity_catalog(
    *,
    warehouse_id: str,
    profile: str,
    sql_file: Path,
    poll_seconds: int,
    timeout_seconds: int,
) -> None:
    statements = _load_sql_statements(sql_file)
    if not statements:
        raise RuntimeError(f"No SQL statements found in {sql_file}")

    print(f"Executing {len(statements)} SQL statements from {sql_file} ...")
    for idx, stmt in enumerate(statements, start=1):
        print(f"[{idx}/{len(statements)}] Submitting statement...")
        _submit_sql_and_wait(
            warehouse_id=warehouse_id,
            statement=stmt,
            profile=profile,
            poll_seconds=poll_seconds,
            timeout_seconds=timeout_seconds,
        )
        print(f"[{idx}/{len(statements)}] SUCCEEDED")


def _rows_from_status(status: dict[str, Any]) -> list[list[Any]]:
    result = status.get("result", {})
    rows = result.get("data_array", [])
    return rows if isinstance(rows, list) else []


def _validate_bronze_contract(
    *,
    warehouse_id: str,
    profile: str,
    catalog: str,
    schema: str,
    poll_seconds: int,
    timeout_seconds: int,
) -> None:
    table_list_sql = ", ".join([f"'{t}'" for t in EXPECTED_TABLES])

    print(f"Checking table existence in {catalog}.{schema} ...")
    table_query = f"""
    SELECT table_name
    FROM {catalog}.information_schema.tables
    WHERE table_schema = '{schema}'
      AND table_name IN ({table_list_sql})
    """
    table_status = _submit_sql_and_wait(
        warehouse_id=warehouse_id,
        statement=table_query,
        profile=profile,
        poll_seconds=poll_seconds,
        timeout_seconds=timeout_seconds,
    )
    existing_tables = {str(row[0]) for row in _rows_from_status(table_status)}

    errors: list[str] = []
    for table in EXPECTED_TABLES:
        if table not in existing_tables:
            errors.append(f"Missing table: {catalog}.{schema}.{table}")

    print("Checking metadata column contract ...")
    col_query = f"""
    SELECT table_name, column_name, UPPER(data_type) AS data_type, UPPER(is_nullable) AS is_nullable
    FROM {catalog}.information_schema.columns
    WHERE table_schema = '{schema}'
      AND table_name IN ({table_list_sql})
      AND column_name IN (
        'snapshot_date', 'ingest_ts_utc', 'source_system', 'request_id',
        'schema_version', 'run_id', 'payload'
      )
    """
    col_status = _submit_sql_and_wait(
        warehouse_id=warehouse_id,
        statement=col_query,
        profile=profile,
        poll_seconds=poll_seconds,
        timeout_seconds=timeout_seconds,
    )
    actual: dict[tuple[str, str], dict[str, str]] = {}
    for row in _rows_from_status(col_status):
        key = (str(row[0]), str(row[1]))
        actual[key] = {"data_type": str(row[2]), "is_nullable": str(row[3])}

    for table in EXPECTED_TABLES:
        for col, expected in EXPECTED_METADATA.items():
            key = (table, col)
            if key not in actual:
                errors.append(f"Missing column: {catalog}.{schema}.{table}.{col}")
                continue
            got = actual[key]
            if got["data_type"] != expected["data_type"]:
                errors.append(
                    f"Type mismatch for {catalog}.{schema}.{table}.{col} "
                    f"(expected={expected['data_type']}, actual={got['data_type']})"
                )
            if got["is_nullable"] != expected["is_nullable"]:
                errors.append(
                    f"Nullability mismatch for {catalog}.{schema}.{table}.{col} "
                    f"(expected={expected['is_nullable']}, actual={got['is_nullable']})"
                )

    if errors:
        msg = "\n".join([f" - {e}" for e in errors])
        raise RuntimeError(f"Bronze contract validation failed:\n{msg}")

    print("Bronze contract validation passed.")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Step 2 orchestrator for Unity Catalog bootstrap + Bronze validation."
    )
    parser.add_argument(
        "--action",
        choices=["bootstrap", "validate", "all"],
        default="all",
        help="Choose which Step 2 action to run.",
    )
    parser.add_argument(
        "--warehouse-id",
        default=os.getenv("DATABRICKS_SQL_WAREHOUSE_ID", ""),
        help="Databricks SQL warehouse id (raw id only).",
    )
    parser.add_argument(
        "--profile",
        default=os.getenv("DATABRICKS_CONFIG_PROFILE", "sef_databricks"),
        help="Databricks CLI profile.",
    )
    parser.add_argument(
        "--catalog",
        default=os.getenv("YOUTUBE_ANALYTICS_CATALOG", "youtube_analytics_dev"),
        help="Unity Catalog name for validation.",
    )
    parser.add_argument(
        "--schema",
        default=os.getenv("YOUTUBE_ANALYTICS_SCHEMA_BRONZE", "bronze"),
        help="Bronze schema name for validation.",
    )
    parser.add_argument(
        "--sql-file",
        default=DEFAULT_SQL_FILE,
        help="Path to bootstrap SQL file.",
    )
    parser.add_argument("--poll-seconds", type=int, default=3)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    return parser.parse_args()


def _require(name: str, value: str) -> str:
    if not value:
        raise ValueError(f"{name} is required. Provide arg or set it in .env.")
    return value


def main() -> None:
    load_dotenv()
    args = _parse_args()
    warehouse_id = _require("warehouse-id", args.warehouse_id)
    if "/" in warehouse_id:
        raise ValueError(
            "warehouse-id must be raw ID only (for example 6bdcbabcbe866b08), not a URL path."
        )

    sql_file = Path(args.sql_file)
    if args.action in {"bootstrap", "all"}:
        _bootstrap_unity_catalog(
            warehouse_id=warehouse_id,
            profile=args.profile,
            sql_file=sql_file,
            poll_seconds=args.poll_seconds,
            timeout_seconds=args.timeout_seconds,
        )

    if args.action in {"validate", "all"}:
        _validate_bronze_contract(
            warehouse_id=warehouse_id,
            profile=args.profile,
            catalog=args.catalog,
            schema=args.schema,
            poll_seconds=args.poll_seconds,
            timeout_seconds=args.timeout_seconds,
        )

if __name__ == "__main__":
    main()
