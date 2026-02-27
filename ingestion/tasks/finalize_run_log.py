"""Finalize run context log with terminal status and timestamps."""

from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

DEFAULT_CATALOG = "youtube_analytics_dev"
BRONZE_SCHEMA = "bronze"
RUN_LOG_TABLE = "run_context_log"
UPSTREAM_TASK_KEY = "init_run_context"
FINALIZE_TASK_KEY = "finalize_run_log"

IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_sql_identifier(name: str, value: str) -> str:
    if not value:
        raise ValueError(f"{name} must be set.")
    if not IDENTIFIER_PATTERN.match(value):
        raise ValueError(f"{name} must be a simple SQL identifier. Got: {value!r}")
    return value


def _escape_sql_string(value: str) -> str:
    return value.replace("'", "''")


def _task_value(key: str) -> str:
    if "dbutils" not in globals():
        return ""
    try:
        value = dbutils.jobs.taskValues.get(taskKey=UPSTREAM_TASK_KEY, key=key, default="")  # type: ignore[name-defined]
        return str(value or "")
    except Exception:
        return ""


def _get_dbutils_tag(candidates: list[str]) -> str:
    if "dbutils" not in globals():
        return ""
    try:
        context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # type: ignore[name-defined]
        tags = context.tags()
    except Exception:
        return ""
    for key in candidates:
        try:
            maybe_value = tags.get(key)
            if hasattr(maybe_value, "isDefined") and maybe_value.isDefined():
                value = maybe_value.get()
                if value:
                    return str(value)
            elif maybe_value:
                return str(maybe_value)
        except Exception:
            continue
    return ""


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Finalize Databricks run context log entry.")
    parser.add_argument("--catalog", required=False, help="Unity Catalog name.")
    parser.add_argument("--job-run-id", "--job_run_id", dest="job_run_id", required=False)
    parser.add_argument("--task-run-id", "--task_run_id", dest="task_run_id", required=False)
    parser.add_argument(
        "--status",
        required=False,
        default="auto",
        choices=["auto", "success", "failed", "cancelled", "unknown"],
        help="Terminal run status. Use auto to infer from Databricks Jobs run state.",
    )
    return parser.parse_args()


def _add_finalize_columns_if_missing(spark: SparkSession, table_fqn: str) -> None:
    existing = {row["col_name"] for row in spark.sql(f"DESCRIBE {table_fqn}").collect() if row["col_name"]}
    required = [
        ("run_status", "STRING"),
        ("finished_ts_utc", "TIMESTAMP"),
        ("finalized_ts_utc", "TIMESTAMP"),
        ("finalize_task_run_id", "STRING"),
        ("finalize_note", "STRING"),
    ]
    missing_defs = [f"{name} {dtype}" for name, dtype in required if name not in existing]
    if missing_defs:
        spark.sql(f"ALTER TABLE {table_fqn} ADD COLUMNS ({', '.join(missing_defs)})")


def _infer_status_from_job_run(job_run_id: str) -> str:
    if not job_run_id:
        return "unknown"

    client = WorkspaceClient()
    run = client.jobs.get_run(run_id=int(job_run_id))
    tasks = getattr(run, "tasks", None) or []
    seen_upstream = False
    for task in tasks:
        task_key = str(getattr(task, "task_key", "") or "")
        if task_key == FINALIZE_TASK_KEY:
            continue
        seen_upstream = True
        task_state = getattr(task, "state", None)
        result_state = str(getattr(task_state, "result_state", "") or "").upper()
        if result_state in {"FAILED", "TIMEDOUT", "UPSTREAM_FAILED", "INTERNAL_ERROR"}:
            return "failed"
        if result_state in {"CANCELED", "CANCELLED"}:
            return "cancelled"
    if seen_upstream:
        return "success"

    state = getattr(run, "state", None)
    result_state = str(getattr(state, "result_state", "") or "").upper()
    life_cycle = str(getattr(state, "life_cycle_state", "") or "").upper()

    if result_state in {"SUCCESS"}:
        return "success"
    if result_state in {"FAILED", "TIMEDOUT", "UPSTREAM_FAILED"}:
        return "failed"
    if result_state in {"CANCELED", "CANCELLED"}:
        return "cancelled"
    if life_cycle in {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}:
        return "failed"
    return "unknown"


def main() -> None:
    args = _parse_args()
    spark = SparkSession.builder.getOrCreate()

    catalog_value = (
        args.catalog
        or _task_value("catalog")
        or os.getenv("YOUTUBE_ANALYTICS_CATALOG", DEFAULT_CATALOG)
    )
    catalog = _validate_sql_identifier("catalog", catalog_value)
    table_fqn = f"`{catalog}`.`{BRONZE_SCHEMA}`.`{RUN_LOG_TABLE}`"

    run_id = _task_value("run_id")
    request_id = _task_value("request_id")
    job_run_id = (
        (str(args.job_run_id) if args.job_run_id else "")
        or _get_dbutils_tag(["jobRunId", "runId", "job_run_id"])
        or os.getenv("DATABRICKS_JOB_RUN_ID", "")
        or _task_value("job_run_id")
    )
    task_run_id = (
        (str(args.task_run_id) if args.task_run_id else "")
        or
        _get_dbutils_tag(["taskRunId", "task_run_id"])
        or os.getenv("DATABRICKS_TASK_RUN_ID", "")
        or _task_value("task_run_id")
    )
    finished_ts_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    _add_finalize_columns_if_missing(spark=spark, table_fqn=table_fqn)

    if not run_id:
        payload = {
            "status": "skipped",
            "reason": "missing_run_id_task_value",
            "table": f"{catalog}.{BRONZE_SCHEMA}.{RUN_LOG_TABLE}",
        }
        print(json.dumps(payload, indent=2))
        return

    final_status = args.status
    if args.status == "auto":
        try:
            final_status = _infer_status_from_job_run(job_run_id=job_run_id)
        except Exception as exc:
            print(f"Could not infer run status from Jobs API: {exc}")
            final_status = "unknown"

    status_sql = _escape_sql_string(final_status)
    finished_sql = _escape_sql_string(finished_ts_utc)
    task_run_sql = _escape_sql_string(task_run_id) if task_run_id else ""
    note_parts = [f"finalized_by={UPSTREAM_TASK_KEY}"]
    if request_id:
        note_parts.append(f"request_id={request_id}")
    if job_run_id:
        note_parts.append(f"job_run_id={job_run_id}")
    note_sql = _escape_sql_string("; ".join(note_parts))

    spark.sql(
        f"""
        UPDATE {table_fqn}
        SET
          run_status = '{status_sql}',
          finished_ts_utc = CAST('{finished_sql}' AS TIMESTAMP),
          finalized_ts_utc = current_timestamp(),
          finalize_task_run_id = {f"'{task_run_sql}'" if task_run_sql else "NULL"},
          finalize_note = '{note_sql}'
        WHERE run_id = '{_escape_sql_string(run_id)}'
        """
    )

    payload = {
        "status": "ok",
        "catalog": catalog,
        "table": f"{catalog}.{BRONZE_SCHEMA}.{RUN_LOG_TABLE}",
        "run_id": run_id,
        "run_status": final_status,
        "finished_ts_utc": finished_ts_utc,
    }
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
