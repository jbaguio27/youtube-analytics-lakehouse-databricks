"""Ingest initial YouTube Analytics API payloads to Bronze Delta tables."""

from __future__ import annotations

import argparse
import json
import os
import uuid
from datetime import date, datetime, timedelta
from typing import Any

import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

DEFAULT_SECRET_SCOPE = "youtube-analytics"
DEFAULT_CATALOG = "youtube_analytics_dev"
BRONZE_SCHEMA = "bronze"
UPSTREAM_TASK_KEY = "init_run_context"
TOKEN_URL = "https://oauth2.googleapis.com/token"
ANALYTICS_REPORTS_URL = "https://youtubeanalytics.googleapis.com/v2/reports"

TABLE_SCHEMA = StructType(
    [
        StructField("snapshot_date", DateType(), nullable=False),
        StructField("ingest_ts_utc", TimestampType(), nullable=False),
        StructField("source_system", StringType(), nullable=False),
        StructField("request_id", StringType(), nullable=False),
        StructField("schema_version", StringType(), nullable=False),
        StructField("run_id", StringType(), nullable=False),
        StructField("payload", StringType(), nullable=False),
    ]
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest YouTube Analytics API to Bronze tables.")
    parser.add_argument("--secret-scope", default=os.getenv("DATABRICKS_SECRET_SCOPE", DEFAULT_SECRET_SCOPE))
    parser.add_argument("--catalog", default=os.getenv("YOUTUBE_ANALYTICS_CATALOG", DEFAULT_CATALOG))
    parser.add_argument("--lookback-days", type=int, default=7)
    parser.add_argument("--start-date", default=os.getenv("YOUTUBE_ANALYTICS_START_DATE", ""))
    parser.add_argument("--end-date", default=os.getenv("YOUTUBE_ANALYTICS_END_DATE", ""))
    return parser.parse_args()


def _parse_iso_date(value: str, arg_name: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"{arg_name} must be in YYYY-MM-DD format. Got '{value}'.") from exc


def _resolve_window(args: argparse.Namespace) -> tuple[date, date, str]:
    start_date_raw = str(args.start_date or "").strip()
    end_date_raw = str(args.end_date or "").strip()
    if start_date_raw.lower() in {"auto", "default", "lookback", "rolling"}:
        start_date_raw = ""
    if end_date_raw.lower() in {"auto", "default", "yesterday"}:
        end_date_raw = ""

    if start_date_raw:
        start_date = _parse_iso_date(start_date_raw, "start-date")
        end_date = _parse_iso_date(end_date_raw, "end-date") if end_date_raw else (date.today() - timedelta(days=1))
        if start_date > end_date:
            raise ValueError(
                f"start-date must be on or before end-date. Got start-date={start_date.isoformat()} "
                f"and end-date={end_date.isoformat()}."
            )
        return start_date, end_date, "explicit_date_range"

    if args.lookback_days <= 0:
        raise ValueError("lookback-days must be greater than 0.")
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=args.lookback_days - 1)
    return start_date, end_date, "rolling_lookback"


def _require_dbutils() -> None:
    if "dbutils" not in globals():
        raise RuntimeError("dbutils is required for this task in Databricks Jobs runtime.")


def _require_secret(scope: str, key: str) -> str:
    try:
        value = dbutils.secrets.get(scope=scope, key=key)  # type: ignore[name-defined]
    except Exception as exc:
        raise RuntimeError(f"Missing Databricks secret '{key}' in scope '{scope}'.") from exc
    if not value:
        raise RuntimeError(f"Secret '{key}' in scope '{scope}' is empty.")
    return str(value)


def _task_value(key: str) -> str:
    try:
        value = dbutils.jobs.taskValues.get(taskKey=UPSTREAM_TASK_KEY, key=key)  # type: ignore[name-defined]
    except Exception as exc:
        raise RuntimeError(
            f"Could not read task value '{key}' from task '{UPSTREAM_TASK_KEY}'."
        ) from exc
    if value is None or str(value) == "":
        raise RuntimeError(f"Task value '{key}' is missing from '{UPSTREAM_TASK_KEY}'.")
    return str(value)


def _task_value_optional(key: str) -> str:
    try:
        value = dbutils.jobs.taskValues.get(taskKey=UPSTREAM_TASK_KEY, key=key)  # type: ignore[name-defined]
    except Exception:
        return ""
    return "" if value is None else str(value)


def _assert_table_exists(spark: SparkSession, table_fqn: str) -> None:
    if not spark.catalog.tableExists(table_fqn):
        raise RuntimeError(
            f"Required Bronze table not found: {table_fqn}. "
            "Run unity_catalog_setup.py --action all before ingestion."
        )


def _delete_existing_run_rows(spark: SparkSession, *, table_fqn: str, run_id: str) -> None:
    safe_run_id = run_id.replace("'", "''")
    spark.sql(f"DELETE FROM {table_fqn} WHERE run_id = '{safe_run_id}'")


def _refresh_access_token(*, client_id: str, client_secret: str, refresh_token: str) -> str:
    response = requests.post(
        TOKEN_URL,
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        },
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    access_token = payload.get("access_token", "")
    if not access_token:
        raise RuntimeError("OAuth token refresh succeeded but access_token was missing.")
    return str(access_token)


def _query_reports(*, access_token: str, params: dict[str, str]) -> dict[str, Any]:
    response = requests.get(
        ANALYTICS_REPORTS_URL,
        params=params,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=60,
    )
    if response.ok:
        payload = response.json()
        payload["request"] = {"params": params}
        payload["status"] = "ok"
        return payload

    error_text = response.text
    try:
        error_body: Any = response.json()
    except Exception:
        error_body = {"raw": error_text}
    return {
        "status": "error",
        "request": {"params": params},
        "error": {
            "http_status": response.status_code,
            "body": error_body,
        },
    }


def _query_with_fallback(
    *,
    access_token: str,
    primary_params: dict[str, str],
    fallback_params: dict[str, str] | None = None,
) -> dict[str, Any]:
    first = _query_reports(access_token=access_token, params=primary_params)
    if first.get("status") == "ok" or fallback_params is None:
        return first

    second = _query_reports(access_token=access_token, params=fallback_params)
    if second.get("status") == "ok":
        second["fallback_used"] = True
        second["primary_error"] = first.get("error")
        return second

    return {
        "status": "error",
        "primary_error": first.get("error"),
        "fallback_error": second.get("error"),
        "request": {
            "primary_params": primary_params,
            "fallback_params": fallback_params,
        },
    }


def _query_with_fallback_chain(
    *,
    access_token: str,
    params_chain: list[dict[str, str]],
) -> dict[str, Any]:
    if not params_chain:
        raise ValueError("params_chain must contain at least one query definition.")

    errors: list[dict[str, Any]] = []
    for i, params in enumerate(params_chain):
        payload = _query_reports(access_token=access_token, params=params)
        if payload.get("status") == "ok":
            if i > 0:
                payload["fallback_used"] = True
                payload["fallback_level"] = i
                payload["previous_errors"] = errors
            return payload
        errors.append({"params": params, "error": payload.get("error")})

    return {
        "status": "error",
        "errors": errors,
        "request": {"params_chain": params_chain},
    }


def _query_channel_daily(*, access_token: str, start_date: date, end_date: date) -> dict[str, Any]:
    params = {
        "ids": "channel==MINE",
        "startDate": start_date.isoformat(),
        "endDate": end_date.isoformat(),
        "metrics": "views,likes,comments,estimatedMinutesWatched,subscribersGained,subscribersLost",
        "dimensions": "day",
        "sort": "day",
    }
    return _query_reports(access_token=access_token, params=params)


def _query_video_daily(*, access_token: str, start_date: date, end_date: date) -> dict[str, Any]:
    params = {
        "ids": "channel==MINE",
        "startDate": start_date.isoformat(),
        "endDate": end_date.isoformat(),
        "metrics": "views,likes,comments,estimatedMinutesWatched,averageViewDuration",
        "dimensions": "day",
        "sort": "day",
    }
    return _query_reports(access_token=access_token, params=params)


def _query_video_dimension_per_video(
    *,
    access_token: str,
    start_date: date,
    end_date: date,
    video_ids: list[str],
    dimension_name: str,
) -> dict[str, Any]:
    bulk_payload = _query_with_fallback_chain(
        access_token=access_token,
        params_chain=[
            {
                "ids": "channel==MINE",
                "startDate": start_date.isoformat(),
                "endDate": end_date.isoformat(),
                "metrics": "views,estimatedMinutesWatched",
                "dimensions": f"day,video,{dimension_name}",
                "sort": f"day,video,{dimension_name}",
            },
            {
                "ids": "channel==MINE",
                "startDate": start_date.isoformat(),
                "endDate": end_date.isoformat(),
                "metrics": "views",
                "dimensions": f"day,video,{dimension_name}",
                "sort": f"day,video,{dimension_name}",
            },
            {
                "ids": "channel==MINE",
                "startDate": start_date.isoformat(),
                "endDate": end_date.isoformat(),
                "metrics": "views,estimatedMinutesWatched",
                "dimensions": f"video,{dimension_name}",
                "sort": f"video,{dimension_name}",
            },
            {
                "ids": "channel==MINE",
                "startDate": start_date.isoformat(),
                "endDate": end_date.isoformat(),
                "metrics": "views",
                "dimensions": f"video,{dimension_name}",
                "sort": f"video,{dimension_name}",
            },
        ],
    )
    if bulk_payload.get("status") == "ok":
        output_rows: list[list[Any]] = []
        header_names = [h.get("name", "") for h in bulk_payload.get("columnHeaders", [])]
        for row in bulk_payload.get("rows", []):
            row_map = {header_names[i]: row[i] for i in range(min(len(header_names), len(row)))}
            output_rows.append(
                [
                    row_map.get("video"),
                    row_map.get("day"),
                    row_map.get(dimension_name),
                    row_map.get("views"),
                    row_map.get("estimatedMinutesWatched"),
                ]
            )
        return {
            "status": "ok",
            "columnHeaders": [
                {"name": "video", "columnType": "DIMENSION", "dataType": "STRING"},
                {"name": "day", "columnType": "DIMENSION", "dataType": "STRING"},
                {"name": dimension_name, "columnType": "DIMENSION", "dataType": "STRING"},
                {"name": "views", "columnType": "METRIC", "dataType": "INTEGER"},
                {"name": "estimatedMinutesWatched", "columnType": "METRIC", "dataType": "INTEGER"},
            ],
            "rows": output_rows,
            "bulk_query_used": True,
            "request": {
                "startDate": start_date.isoformat(),
                "endDate": end_date.isoformat(),
                "dimension": dimension_name,
                "strategy": "channel_level_video_dimension",
            },
        }

    if not video_ids:
        return {
            "status": "ok",
            "columnHeaders": [
                {"name": "video", "columnType": "DIMENSION", "dataType": "STRING"},
                {"name": "day", "columnType": "DIMENSION", "dataType": "STRING"},
                {"name": dimension_name, "columnType": "DIMENSION", "dataType": "STRING"},
                {"name": "views", "columnType": "METRIC", "dataType": "INTEGER"},
                {"name": "estimatedMinutesWatched", "columnType": "METRIC", "dataType": "INTEGER"},
            ],
            "rows": [],
            "request": {
                "video_count": 0,
                "startDate": start_date.isoformat(),
                "endDate": end_date.isoformat(),
                "dimension": dimension_name,
            },
        }

    output_rows: list[list[Any]] = []
    errors: list[dict[str, Any]] = []
    for video_id in video_ids:
        payload = _query_with_fallback_chain(
            access_token=access_token,
            params_chain=[
                {
                    "ids": "channel==MINE",
                    "startDate": start_date.isoformat(),
                    "endDate": end_date.isoformat(),
                    "metrics": "views,estimatedMinutesWatched",
                    "dimensions": f"day,{dimension_name}",
                    "filters": f"video=={video_id}",
                    "sort": f"day,{dimension_name}",
                },
                {
                    "ids": "channel==MINE",
                    "startDate": start_date.isoformat(),
                    "endDate": end_date.isoformat(),
                    "metrics": "views",
                    "dimensions": f"day,{dimension_name}",
                    "filters": f"video=={video_id}",
                    "sort": f"day,{dimension_name}",
                },
                {
                    "ids": "channel==MINE",
                    "startDate": start_date.isoformat(),
                    "endDate": end_date.isoformat(),
                    "metrics": "views",
                    "dimensions": dimension_name,
                    "filters": f"video=={video_id}",
                    "sort": dimension_name,
                },
            ],
        )
        if payload.get("status") != "ok":
            errors.append(
                {
                    "video_id": video_id,
                    "errors": payload.get("errors"),
                }
            )
            continue

        header_names = [h.get("name", "") for h in payload.get("columnHeaders", [])]
        for row in payload.get("rows", []):
            row_map = {header_names[i]: row[i] for i in range(min(len(header_names), len(row)))}
            output_rows.append(
                [
                    video_id,
                    row_map.get("day"),
                    row_map.get(dimension_name),
                    row_map.get("views"),
                    row_map.get("estimatedMinutesWatched"),
                ]
            )

    status = "ok" if not errors else ("partial_error" if output_rows else "error")
    return {
        "status": status,
        "columnHeaders": [
            {"name": "video", "columnType": "DIMENSION", "dataType": "STRING"},
            {"name": "day", "columnType": "DIMENSION", "dataType": "STRING"},
            {"name": dimension_name, "columnType": "DIMENSION", "dataType": "STRING"},
            {"name": "views", "columnType": "METRIC", "dataType": "INTEGER"},
            {"name": "estimatedMinutesWatched", "columnType": "METRIC", "dataType": "INTEGER"},
        ],
        "rows": output_rows,
        "errors": errors,
        "bulk_query_error": bulk_payload.get("errors"),
        "request": {
            "video_count": len(video_ids),
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
            "dimension": dimension_name,
            "filters": "video==VIDEO_ID (per request)",
        },
    }


def _query_video_traffic_source_daily(
    *, access_token: str, start_date: date, end_date: date, video_ids: list[str]
) -> dict[str, Any]:
    return _query_video_dimension_per_video(
        access_token=access_token,
        start_date=start_date,
        end_date=end_date,
        video_ids=video_ids,
        dimension_name="insightTrafficSourceType",
    )


def _query_video_country_daily(
    *, access_token: str, start_date: date, end_date: date, video_ids: list[str]
) -> dict[str, Any]:
    return _query_video_dimension_per_video(
        access_token=access_token,
        start_date=start_date,
        end_date=end_date,
        video_ids=video_ids,
        dimension_name="country",
    )


def _query_video_device_daily(
    *, access_token: str, start_date: date, end_date: date, video_ids: list[str]
) -> dict[str, Any]:
    return _query_video_dimension_per_video(
        access_token=access_token,
        start_date=start_date,
        end_date=end_date,
        video_ids=video_ids,
        dimension_name="deviceType",
    )


def _latest_video_ids_for_channel(
    spark: SparkSession, *, catalog: str, bronze_schema: str = BRONZE_SCHEMA
) -> list[str]:
    table_fqn = f"{catalog}.{bronze_schema}.videos_raw"
    latest_rows = spark.sql(
        f"""
        SELECT payload
        FROM {table_fqn}
        ORDER BY snapshot_date DESC, ingest_ts_utc DESC
        LIMIT 1
        """
    ).collect()
    if not latest_rows:
        return []

    latest_payload = str(latest_rows[0]["payload"])
    parsed = json.loads(latest_payload)
    items = parsed.get("items", [])
    video_ids: list[str] = []
    for item in items:
        video_id = item.get("id")
        if video_id:
            video_ids.append(str(video_id))
    return sorted(set(video_ids))


def _query_video_daily_per_video(
    *,
    access_token: str,
    start_date: date,
    end_date: date,
    video_ids: list[str],
) -> dict[str, Any]:
    if not video_ids:
        return {
            "status": "ok",
            "columnHeaders": [
                {"name": "video", "columnType": "DIMENSION", "dataType": "STRING"},
                {"name": "day", "columnType": "DIMENSION", "dataType": "STRING"},
                {"name": "views", "columnType": "METRIC", "dataType": "INTEGER"},
                {"name": "likes", "columnType": "METRIC", "dataType": "INTEGER"},
                {"name": "comments", "columnType": "METRIC", "dataType": "INTEGER"},
                {
                    "name": "estimatedMinutesWatched",
                    "columnType": "METRIC",
                    "dataType": "INTEGER",
                },
                {
                    "name": "averageViewDuration",
                    "columnType": "METRIC",
                    "dataType": "FLOAT",
                },
            ],
            "rows": [],
            "request": {
                "video_count": 0,
                "startDate": start_date.isoformat(),
                "endDate": end_date.isoformat(),
            },
        }

    primary_metrics = "views,likes,comments,estimatedMinutesWatched,averageViewDuration"
    fallback_metrics = "views,estimatedMinutesWatched"
    output_rows: list[list[Any]] = []
    errors: list[dict[str, Any]] = []

    for video_id in video_ids:
        primary = {
            "ids": "channel==MINE",
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
            "metrics": primary_metrics,
            "dimensions": "day",
            "filters": f"video=={video_id}",
            "sort": "day",
        }
        fallback = {
            "ids": "channel==MINE",
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
            "metrics": fallback_metrics,
            "dimensions": "day",
            "filters": f"video=={video_id}",
            "sort": "day",
        }
        payload = _query_with_fallback(
            access_token=access_token,
            primary_params=primary,
            fallback_params=fallback,
        )
        if payload.get("status") != "ok":
            errors.append(
                {
                    "video_id": video_id,
                    "primary_error": payload.get("primary_error"),
                    "fallback_error": payload.get("fallback_error"),
                }
            )
            continue

        header_names = [h.get("name", "") for h in payload.get("columnHeaders", [])]
        rows = payload.get("rows", [])
        for row in rows:
            row_map = {header_names[i]: row[i] for i in range(min(len(header_names), len(row)))}
            output_rows.append(
                [
                    video_id,
                    row_map.get("day"),
                    row_map.get("views"),
                    row_map.get("likes"),
                    row_map.get("comments"),
                    row_map.get("estimatedMinutesWatched"),
                    row_map.get("averageViewDuration"),
                ]
            )

    status = "ok" if not errors else ("partial_error" if output_rows else "error")
    return {
        "status": status,
        "columnHeaders": [
            {"name": "video", "columnType": "DIMENSION", "dataType": "STRING"},
            {"name": "day", "columnType": "DIMENSION", "dataType": "STRING"},
            {"name": "views", "columnType": "METRIC", "dataType": "INTEGER"},
            {"name": "likes", "columnType": "METRIC", "dataType": "INTEGER"},
            {"name": "comments", "columnType": "METRIC", "dataType": "INTEGER"},
            {"name": "estimatedMinutesWatched", "columnType": "METRIC", "dataType": "INTEGER"},
            {"name": "averageViewDuration", "columnType": "METRIC", "dataType": "FLOAT"},
        ],
        "rows": output_rows,
        "errors": errors,
        "request": {
            "video_count": len(video_ids),
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
            "dimensions": "day",
            "filters": "video==VIDEO_ID (per request)",
        },
    }


def _build_row(*, context: dict[str, str], payload: dict, request_id: str) -> tuple:
    return (
        datetime.fromisoformat(context["snapshot_date"]).date(),
        datetime.fromisoformat(context["ingest_ts_utc"]),
        context["source_system"],
        request_id,
        context["schema_version"],
        context["run_id"],
        json.dumps(payload, sort_keys=True),
    )


def _write_payload(spark: SparkSession, *, table_fqn: str, row: tuple, run_id: str) -> None:
    _assert_table_exists(spark, table_fqn)
    _delete_existing_run_rows(spark, table_fqn=table_fqn, run_id=run_id)
    spark.createDataFrame([row], schema=TABLE_SCHEMA).write.mode("append").saveAsTable(table_fqn)


def main() -> None:
    args = _parse_args()
    start_date, end_date, window_mode = _resolve_window(args)

    _require_dbutils()
    scope = args.secret_scope
    client_id = _require_secret(scope, "yt_client_id")
    client_secret = _require_secret(scope, "yt_client_secret")
    refresh_token = _require_secret(scope, "yt_refresh_token")

    context = {
        "run_id": _task_value("run_id"),
        "request_id": _task_value("request_id"),
        "snapshot_date": _task_value("snapshot_date"),
        "ingest_ts_utc": _task_value("ingest_ts_utc"),
        "source_system": _task_value("source_system"),
        "schema_version": _task_value("schema_version"),
    }
    catalog = _task_value_optional("catalog") or args.catalog
    spark = SparkSession.builder.getOrCreate()

    access_token = _refresh_access_token(
        client_id=client_id, client_secret=client_secret, refresh_token=refresh_token
    )
    video_ids = _latest_video_ids_for_channel(spark, catalog=catalog)

    channel_daily_payload = _query_channel_daily(
        access_token=access_token, start_date=start_date, end_date=end_date
    )
    channel_daily_payload["window"] = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "lookback_days": args.lookback_days,
        "mode": window_mode,
    }

    channel_table = f"{catalog}.{BRONZE_SCHEMA}.analytics_channel_daily_raw"
    _write_payload(
        spark,
        table_fqn=channel_table,
        row=_build_row(context=context, payload=channel_daily_payload, request_id=str(uuid.uuid4())),
        run_id=context["run_id"],
    )

    video_daily_payload = _query_video_daily_per_video(
        access_token=access_token,
        start_date=start_date,
        end_date=end_date,
        video_ids=video_ids,
    )
    video_daily_payload["window"] = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "lookback_days": args.lookback_days,
        "mode": window_mode,
    }
    _write_payload(
        spark,
        table_fqn=f"{catalog}.{BRONZE_SCHEMA}.analytics_video_daily_raw",
        row=_build_row(context=context, payload=video_daily_payload, request_id=str(uuid.uuid4())),
        run_id=context["run_id"],
    )

    traffic_payload = _query_video_traffic_source_daily(
        access_token=access_token,
        start_date=start_date,
        end_date=end_date,
        video_ids=video_ids,
    )
    traffic_payload["window"] = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "lookback_days": args.lookback_days,
        "mode": window_mode,
    }
    _write_payload(
        spark,
        table_fqn=f"{catalog}.{BRONZE_SCHEMA}.analytics_video_traffic_source_daily_raw",
        row=_build_row(context=context, payload=traffic_payload, request_id=str(uuid.uuid4())),
        run_id=context["run_id"],
    )

    country_payload = _query_video_country_daily(
        access_token=access_token,
        start_date=start_date,
        end_date=end_date,
        video_ids=video_ids,
    )
    country_payload["window"] = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "lookback_days": args.lookback_days,
        "mode": window_mode,
    }
    _write_payload(
        spark,
        table_fqn=f"{catalog}.{BRONZE_SCHEMA}.analytics_video_country_daily_raw",
        row=_build_row(context=context, payload=country_payload, request_id=str(uuid.uuid4())),
        run_id=context["run_id"],
    )

    device_payload = _query_video_device_daily(
        access_token=access_token,
        start_date=start_date,
        end_date=end_date,
        video_ids=video_ids,
    )
    device_payload["window"] = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "lookback_days": args.lookback_days,
        "mode": window_mode,
    }
    _write_payload(
        spark,
        table_fqn=f"{catalog}.{BRONZE_SCHEMA}.analytics_video_device_daily_raw",
        row=_build_row(context=context, payload=device_payload, request_id=str(uuid.uuid4())),
        run_id=context["run_id"],
    )

    print(
        json.dumps(
            {
                "status": "ok",
                "catalog": catalog,
                "secret_scope": scope,
                "channel_daily_status": channel_daily_payload.get("status"),
                "video_daily_status": video_daily_payload.get("status"),
                "video_traffic_source_status": traffic_payload.get("status"),
                "video_country_status": country_payload.get("status"),
                "video_device_status": device_payload.get("status"),
                "channel_daily_rows": len(channel_daily_payload.get("rows", [])),
                "video_daily_rows": len(video_daily_payload.get("rows", [])),
                "video_traffic_source_rows": len(traffic_payload.get("rows", [])),
                "video_country_rows": len(country_payload.get("rows", [])),
                "video_device_rows": len(device_payload.get("rows", [])),
                "window": {
                    "mode": window_mode,
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "lookback_days": args.lookback_days,
                },
                "written_tables": [
                    "analytics_channel_daily_raw",
                    "analytics_video_daily_raw",
                    "analytics_video_traffic_source_daily_raw",
                    "analytics_video_country_daily_raw",
                    "analytics_video_device_daily_raw",
                ],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
