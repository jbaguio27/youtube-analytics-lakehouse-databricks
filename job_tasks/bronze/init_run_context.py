"""Initialize shared run context for the Databricks job."""

from __future__ import annotations

import argparse
import json
import os
import re
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

DEFAULT_CATALOG = "youtube_analytics_dev"
BRONZE_SCHEMA = "bronze"
RUN_LOG_TABLE = "run_context_log"
SCHEMA_VERSION = "v1"
SOURCE_SYSTEM = "youtube"

IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_sql_identifier(name: str, value: str) -> str:
    if not value:
        raise ValueError(f"{name} must be set.")
    if not IDENTIFIER_PATTERN.match(value):
        raise ValueError(
            f"{name} must be a simple SQL identifier. Got: {value!r}"
        )
    return value


def _get_dbutils_tag(candidates: list[str]) -> str:
    if "dbutils" not in globals():
        return ""

    try:
        context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # type: ignore[name-defined]
        tags = context.tags()
    except Exception as exc:
        # On some serverless runtimes, context.tags() is not whitelisted.
        # In that case we silently fall back to other sources.
        if "not whitelisted" not in str(exc):
            print(f"Could not read dbutils context tags: {exc}")
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


def _get_current_user(spark: SparkSession) -> str:
    row = spark.sql("SELECT current_user() AS user_name").first()
    if row and row["user_name"]:
        return str(row["user_name"])
    return ""


def _build_run_context(spark: SparkSession) -> dict[str, str]:
    now_utc = datetime.now(timezone.utc)
    job_id = _get_dbutils_tag(["jobId", "job_id"]) or os.getenv("DATABRICKS_JOB_ID", "")
    job_run_id = _get_dbutils_tag(["jobRunId", "runId", "job_run_id"]) or os.getenv(
        "DATABRICKS_JOB_RUN_ID", ""
    )
    task_run_id = _get_dbutils_tag(["taskRunId", "task_run_id"]) or os.getenv(
        "DATABRICKS_TASK_RUN_ID", ""
    )
    created_by = _get_dbutils_tag(["user", "userName", "user_name"]) or os.getenv(
        "DATABRICKS_USER", ""
    )
    if not created_by:
        created_by = _get_current_user(spark)

    return {
        "run_id": str(uuid.uuid4()),
        "request_id": str(uuid.uuid4()),
        "snapshot_date": now_utc.date().isoformat(),
        "ingest_ts_utc": now_utc.replace(microsecond=0).isoformat(),
        "source_system": SOURCE_SYSTEM,
        "schema_version": SCHEMA_VERSION,
        "job_id": job_id,
        "job_run_id": job_run_id,
        "task_run_id": task_run_id,
        "created_by": created_by,
    }


def _write_run_log(spark: SparkSession, catalog: str, context: dict[str, str]) -> None:
    table_fqn = f"`{catalog}`.`{BRONZE_SCHEMA}`.`{RUN_LOG_TABLE}`"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{BRONZE_SCHEMA}`")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_fqn} (
          run_id STRING NOT NULL,
          request_id STRING NOT NULL,
          snapshot_date DATE NOT NULL,
          ingest_ts_utc TIMESTAMP NOT NULL,
          source_system STRING NOT NULL,
          schema_version STRING NOT NULL,
          job_id STRING,
          job_run_id STRING,
          task_run_id STRING,
          created_by STRING,
          context_json STRING NOT NULL
        )
        USING DELTA
        PARTITIONED BY (snapshot_date)
        """
    )

    schema = StructType(
        [
            StructField("run_id", StringType(), nullable=False),
            StructField("request_id", StringType(), nullable=False),
            StructField("snapshot_date", DateType(), nullable=False),
            StructField("ingest_ts_utc", TimestampType(), nullable=False),
            StructField("source_system", StringType(), nullable=False),
            StructField("schema_version", StringType(), nullable=False),
            StructField("job_id", StringType(), nullable=True),
            StructField("job_run_id", StringType(), nullable=True),
            StructField("task_run_id", StringType(), nullable=True),
            StructField("created_by", StringType(), nullable=True),
            StructField("context_json", StringType(), nullable=False),
        ]
    )

    row = [
        (
            context["run_id"],
            context["request_id"],
            datetime.fromisoformat(context["snapshot_date"]).date(),
            datetime.fromisoformat(context["ingest_ts_utc"]),
            context["source_system"],
            context["schema_version"],
            context["job_id"] or None,
            context["job_run_id"] or None,
            context["task_run_id"] or None,
            context["created_by"] or None,
            json.dumps(context, sort_keys=True),
        )
    ]
    spark.createDataFrame(row, schema=schema).write.mode("append").saveAsTable(table_fqn)


def _publish_task_values(context: dict[str, str], catalog: str) -> None:
    if "dbutils" not in globals():
        print("dbutils is not available; skipping taskValues publish.")
        return

    values = {
        "run_id": context["run_id"],
        "request_id": context["request_id"],
        "snapshot_date": context["snapshot_date"],
        "ingest_ts_utc": context["ingest_ts_utc"],
        "source_system": context["source_system"],
        "schema_version": context["schema_version"],
        "catalog": catalog,
        "bronze_schema": BRONZE_SCHEMA,
        "run_log_table": RUN_LOG_TABLE,
    }
    for key, value in values.items():
        dbutils.jobs.taskValues.set(key=key, value=value)  # type: ignore[name-defined]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Initialize Databricks run context.")
    parser.add_argument(
        "--catalog",
        required=False,
        help="Unity Catalog name for run logging (for example youtube_analytics_dev).",
    )
    parser.add_argument("--job-id", "--job_id", dest="job_id", required=False)
    parser.add_argument("--job-run-id", "--job_run_id", dest="job_run_id", required=False)
    parser.add_argument("--task-run-id", "--task_run_id", dest="task_run_id", required=False)
    parser.add_argument("--created-by", "--created_by", dest="created_by", required=False)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    catalog_value = args.catalog or os.getenv("YOUTUBE_ANALYTICS_CATALOG", DEFAULT_CATALOG)
    catalog = _validate_sql_identifier("catalog", catalog_value)
    spark = SparkSession.builder.getOrCreate()
    context = _build_run_context(spark=spark)
    if args.job_id:
        context["job_id"] = str(args.job_id)
    if args.job_run_id:
        context["job_run_id"] = str(args.job_run_id)
    if args.task_run_id:
        context["task_run_id"] = str(args.task_run_id)
    if args.created_by:
        context["created_by"] = str(args.created_by)
    _write_run_log(spark=spark, catalog=catalog, context=context)
    _publish_task_values(context=context, catalog=catalog)
    print(json.dumps({"status": "ok", "run_context": context}, indent=2))


if __name__ == "__main__":
    main()
