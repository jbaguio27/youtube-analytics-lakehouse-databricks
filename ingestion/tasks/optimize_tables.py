"""Optimize Delta tables after ingestion and transformations."""

from __future__ import annotations

import argparse
import json
import os
import re
from typing import Iterable

from pyspark.sql import SparkSession

DEFAULT_CATALOG = "youtube_analytics_dev"
UPSTREAM_TASK_KEY = "init_run_context"
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

TABLES_TO_OPTIMIZE: dict[str, list[str]] = {
    "bronze": [
        "run_context_log",
        "channels_raw",
        "playlist_items_raw",
        "videos_raw",
        "analytics_channel_daily_raw",
        "analytics_video_daily_raw",
        "analytics_video_traffic_source_daily_raw",
        "analytics_video_country_daily_raw",
        "analytics_video_device_daily_raw",
    ],
    "silver": [
        "silver_channels",
        "silver_videos",
        "silver_video_stats_snapshot",
        "silver_video_metadata_scd2",
        "fact_channel_daily_metrics",
        "fact_video_daily_metrics",
        "fact_video_traffic_source_metrics",
        "fact_video_country_metrics",
        "fact_video_device_metrics",
        "dim_date",
        "dim_traffic_source",
        "dim_country",
        "dim_device",
        "dim_country_reference",
    ],
    "silver_physical": [
        "silver_channels",
        "silver_videos",
        "silver_video_stats_snapshot",
        "silver_video_metadata_scd2",
        "fact_channel_daily_metrics",
        "fact_video_daily_metrics",
        "fact_video_traffic_source_metrics",
        "fact_video_country_metrics",
        "fact_video_device_metrics",
        "dim_date",
        "dim_traffic_source",
        "dim_country",
        "dim_device",
        "dim_country_reference",
    ],
    "gold": [
        "gold_channel_daily_summary",
        "gold_video_daily_summary",
        "gold_video_country_daily_summary",
        "gold_video_device_daily_summary",
        "gold_video_traffic_source_daily_summary",
    ],
}

SILVER_OBJECTS = TABLES_TO_OPTIMIZE["silver"]


def _validate_identifier(value: str, name: str) -> str:
    if not value:
        raise ValueError(f"{name} must be set.")
    if not IDENTIFIER_PATTERN.match(value):
        raise ValueError(f"{name} must be a SQL identifier. Got: {value!r}")
    return value


def _task_value_optional(key: str) -> str:
    if "dbutils" not in globals():
        return ""
    try:
        value = dbutils.jobs.taskValues.get(taskKey=UPSTREAM_TASK_KEY, key=key)  # type: ignore[name-defined]
    except Exception:
        return ""
    return "" if value is None else str(value)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Optimize Delta tables.")
    parser.add_argument("--catalog", default=os.getenv("YOUTUBE_ANALYTICS_CATALOG", DEFAULT_CATALOG))
    parser.add_argument("--source-schema", default="silver")
    parser.add_argument("--target-schema", default="silver_physical")
    parser.add_argument(
        "--schemas",
        default="bronze,silver,silver_physical,gold",
        help="Comma-separated schemas to optimize (default: bronze,silver,silver_physical,gold).",
    )
    parser.add_argument(
        "--skip-materialize",
        action="store_true",
        help="Skip materializing Silver views into silver_physical before optimize.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail task if any OPTIMIZE statement fails.",
    )
    return parser.parse_args()


def _table_type(spark: SparkSession, *, catalog: str, schema: str, table: str) -> str | None:
    try:
        table_meta = spark.catalog.getTable(f"{catalog}.{schema}.{table}")
    except Exception:
        return None
    return str(table_meta.tableType or "").upper()


def _iter_targets(schemas: Iterable[str]) -> Iterable[tuple[str, str]]:
    for schema in schemas:
        for table in TABLES_TO_OPTIMIZE.get(schema, []):
            yield schema, table


def _materialize_silver_tables(
    spark: SparkSession,
    *,
    catalog: str,
    source_schema: str,
    target_schema: str,
    strict: bool,
) -> tuple[list[str], list[dict[str, str]], list[dict[str, str]]]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{target_schema}`")

    materialized: list[str] = []
    skipped: list[dict[str, str]] = []
    failed: list[dict[str, str]] = []

    for object_name in SILVER_OBJECTS:
        source_fqn = f"`{catalog}`.`{source_schema}`.`{object_name}`"
        target_fqn = f"`{catalog}`.`{target_schema}`.`{object_name}`"
        try:
            if not spark.catalog.tableExists(f"{catalog}.{source_schema}.{object_name}"):
                skipped.append({"source": source_fqn, "reason": "source_not_found"})
                continue
            spark.sql(f"CREATE OR REPLACE TABLE {target_fqn} AS SELECT * FROM {source_fqn}")
            materialized.append(target_fqn)
        except Exception as exc:
            failed.append({"source": source_fqn, "target": target_fqn, "error": str(exc)})
            if strict:
                raise RuntimeError(f"Failed to materialize {source_fqn} -> {target_fqn}") from exc

    return materialized, skipped, failed


def main() -> None:
    args = _parse_args()
    catalog_value = _task_value_optional("catalog") or args.catalog
    catalog = _validate_identifier(catalog_value, "catalog")
    source_schema = _validate_identifier(args.source_schema, "source_schema")
    target_schema = _validate_identifier(args.target_schema, "target_schema")

    schema_names = [s.strip().lower() for s in args.schemas.split(",") if s.strip()]
    for schema in schema_names:
        _validate_identifier(schema, "schema")

    spark = SparkSession.builder.getOrCreate()

    materialized: list[str] = []
    materialize_skipped: list[dict[str, str]] = []
    materialize_failed: list[dict[str, str]] = []
    if not args.skip_materialize:
        materialized, materialize_skipped, materialize_failed = _materialize_silver_tables(
            spark,
            catalog=catalog,
            source_schema=source_schema,
            target_schema=target_schema,
            strict=args.strict,
        )

    optimized: list[str] = []
    skipped: list[dict[str, str]] = []
    failed: list[dict[str, str]] = []

    for schema, table in _iter_targets(schema_names):
        table_type = _table_type(spark, catalog=catalog, schema=schema, table=table)
        table_fqn = f"`{catalog}`.`{schema}`.`{table}`"
        if table_type is None:
            skipped.append({"table": table_fqn, "reason": "not_found"})
            continue
        if table_type in {"VIEW", "MATERIALIZED_VIEW"}:
            skipped.append({"table": table_fqn, "reason": f"unsupported_table_type:{table_type}"})
            continue

        try:
            spark.sql(f"OPTIMIZE {table_fqn}")
            optimized.append(table_fqn)
        except Exception as exc:
            failed.append({"table": table_fqn, "error": str(exc)})
            if args.strict:
                raise RuntimeError(f"OPTIMIZE failed for {table_fqn}") from exc

    print(
        json.dumps(
            {
                "status": "ok" if not failed else "partial_error",
                "catalog": catalog,
                "source_schema": source_schema,
                "target_schema": target_schema,
                "schemas": schema_names,
                "materialized_count": len(materialized),
                "materialized_skipped_count": len(materialize_skipped),
                "materialized_failed_count": len(materialize_failed),
                "materialized": materialized,
                "materialize_skipped": materialize_skipped,
                "materialize_failed": materialize_failed,
                "optimized_count": len(optimized),
                "skipped_count": len(skipped),
                "failed_count": len(failed),
                "optimized": optimized,
                "skipped": skipped,
                "failed": failed,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
