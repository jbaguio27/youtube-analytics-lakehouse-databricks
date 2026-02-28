"""Ingest initial YouTube Data API payloads to Bronze Delta tables."""

from __future__ import annotations

import argparse
import json
import os
import uuid
from datetime import datetime

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
DATA_API_BASE = "https://www.googleapis.com/youtube/v3"

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
    parser = argparse.ArgumentParser(description="Ingest YouTube Data API to Bronze tables.")
    parser.add_argument("--secret-scope", default=os.getenv("DATABRICKS_SECRET_SCOPE", DEFAULT_SECRET_SCOPE))
    parser.add_argument("--catalog", default=os.getenv("YOUTUBE_ANALYTICS_CATALOG", DEFAULT_CATALOG))
    return parser.parse_args()


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


def _api_get_json(*, access_token: str, path: str, params: dict[str, str]) -> dict:
    response = requests.get(
        f"{DATA_API_BASE}/{path}",
        params=params,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


def _fetch_all_playlist_items(*, access_token: str, uploads_playlist_id: str) -> dict:
    items: list[dict] = []
    next_page_token = ""
    page_count = 0

    while True:
        params = {
            "part": "snippet,contentDetails,status",
            "playlistId": uploads_playlist_id,
            "maxResults": "50",
        }
        if next_page_token:
            params["pageToken"] = next_page_token

        page_payload = _api_get_json(access_token=access_token, path="playlistItems", params=params)
        page_items = page_payload.get("items", [])
        if isinstance(page_items, list):
            items.extend(page_items)

        page_count += 1
        next_page_token = str(page_payload.get("nextPageToken", "") or "")
        if not next_page_token:
            break

    return {
        "items": items,
        "item_count": len(items),
        "page_count": page_count,
        "playlist_id": uploads_playlist_id,
    }


def _fetch_videos_by_ids(*, access_token: str, video_ids: list[str]) -> dict:
    items: list[dict] = []
    chunk_size = 50

    for i in range(0, len(video_ids), chunk_size):
        chunk = video_ids[i : i + chunk_size]
        page_payload = _api_get_json(
            access_token=access_token,
            path="videos",
            params={
                "part": "id,snippet,contentDetails,statistics,status,topicDetails",
                "id": ",".join(chunk),
                "maxResults": "50",
            },
        )
        page_items = page_payload.get("items", [])
        if isinstance(page_items, list):
            items.extend(page_items)

    return {
        "items": items,
        "item_count": len(items),
        "requested_video_count": len(video_ids),
        "chunk_size": chunk_size,
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

    channels_response = _api_get_json(
        access_token=access_token,
        path="channels",
        params={"part": "id,snippet,contentDetails,statistics", "mine": "true"},
    )
    channels_table = f"{catalog}.{BRONZE_SCHEMA}.channels_raw"
    _write_payload(
        spark,
        table_fqn=channels_table,
        row=_build_row(context=context, payload=channels_response, request_id=str(uuid.uuid4())),
        run_id=context["run_id"],
    )

    items = channels_response.get("items", [])
    uploads_playlist_id = ""
    if items:
        uploads_playlist_id = (
            items[0]
            .get("contentDetails", {})
            .get("relatedPlaylists", {})
            .get("uploads", "")
        )

    playlist_payload: dict
    if uploads_playlist_id:
        playlist_payload = _fetch_all_playlist_items(
            access_token=access_token,
            uploads_playlist_id=uploads_playlist_id,
        )
    else:
        playlist_payload = {
            "items": [],
            "warning": "uploads playlist id missing from channels response.",
        }
    playlist_table = f"{catalog}.{BRONZE_SCHEMA}.playlist_items_raw"
    _write_payload(
        spark,
        table_fqn=playlist_table,
        row=_build_row(context=context, payload=playlist_payload, request_id=str(uuid.uuid4())),
        run_id=context["run_id"],
    )

    video_ids = []
    for item in playlist_payload.get("items", []):
        video_id = item.get("contentDetails", {}).get("videoId")
        if video_id:
            video_ids.append(str(video_id))
    video_ids = sorted(set(video_ids))

    videos_payload: dict
    if video_ids:
        videos_payload = _fetch_videos_by_ids(access_token=access_token, video_ids=video_ids)
    else:
        videos_payload = {"items": [], "warning": "no video ids found in playlist items response."}
    videos_table = f"{catalog}.{BRONZE_SCHEMA}.videos_raw"
    _write_payload(
        spark,
        table_fqn=videos_table,
        row=_build_row(context=context, payload=videos_payload, request_id=str(uuid.uuid4())),
        run_id=context["run_id"],
    )

    print(
        json.dumps(
            {
                "status": "ok",
                "catalog": catalog,
                "secret_scope": scope,
                "written_tables": ["channels_raw", "playlist_items_raw", "videos_raw"],
                "channels_item_count": len(channels_response.get("items", [])),
                "playlist_items_count": len(playlist_payload.get("items", [])),
                "videos_count": len(videos_payload.get("items", [])),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
