# YouTube Analytics Lakehouse

## Overview
This project builds a Databricks-native YouTube Analytics lakehouse pipeline.

Scope of the baseline:
- OAuth setup and secure secret storage in Databricks Secrets
- Bronze ingestion from YouTube Data API v3 and YouTube Analytics API
- Bronze to Silver transformations with Lakeflow Declarative Pipelines
- Gold models, tests, and docs with dbt
- Single Databricks Job orchestration
- Deployment with Databricks Asset Bundles

This baseline intentionally provides project structure and deployment wiring only. Ingestion and transformation logic will be implemented next.

## Architecture
1. Local one-time OAuth consent flow obtains `yt_refresh_token`.
2. Secrets are stored in Databricks Secret Scope:
   - `yt_client_id`
   - `yt_client_secret`
   - `yt_refresh_token`
3. Databricks job tasks run in sequence:
   - `init_run_context`
   - `ingest_data_api_to_bronze`
   - `ingest_analytics_api_to_bronze`
   - `run_lakeflow_pipeline`
   - `dbt_run_gold`
   - `dbt_test`
   - `optimize_tables`
   - `finalize_run_log`
4. Bronze and Silver reside in Unity Catalog (`youtube_analytics_dev` / `youtube_analytics`).
5. dbt builds Gold models from Silver sources.

## Data Model
Catalogs:
- `youtube_analytics_dev`
- `youtube_analytics`

Schemas:
- `bronze`
- `silver`
- `gold`

Silver entities:
- `silver_channels`
- `silver_videos`
- `silver_video_metadata_scd2`
- `silver_video_stats_snapshot`
- `dim_date`
- `dim_traffic_source`
- `dim_country`
- `dim_device`

Facts:
- `fact_channel_daily_metrics`
- `fact_video_daily_metrics`
- `fact_video_traffic_source_metrics`
- `fact_video_country_metrics`
- `fact_video_device_metrics`

## How to Run
### Local setup (PowerShell + uv)
```powershell
uv venv .venv
.\.venv\Scripts\Activate.ps1
uv sync
```

### Local .env file (PowerShell)
```powershell
Copy-Item .\.env.example .\.env -Force
```
Use `.env` for local defaults only. Keep OAuth/runtime secrets in Databricks Secrets.

### Databricks bundle workflow (PowerShell)
```powershell
# Validate
uv run databricks bundle validate --target dev --profile sef_databricks

# Deploy to dev
uv run databricks bundle deploy --target dev --profile sef_databricks
```

### Unity Catalog bootstrap (PowerShell)
```powershell
# 1) Pick a SQL warehouse ID (copy the `id` field only)
databricks api get /api/2.0/sql/warehouses --profile sef_databricks -o json

# 2) Run bootstrap script (example warehouse id format: 6bdcbabcbe866b08)
.\scripts\bootstrap_unity_catalog.ps1 -WarehouseId "<YOUR_SQL_WAREHOUSE_ID>" -Profile sef_databricks
```

## CI/CD
Recommended CI steps:
1. `uv sync --frozen`
2. `uv run pytest`
3. `uv run databricks bundle validate --target dev --profile sef_databricks`
4. Deploy by environment (`dev`, `prod`) with Databricks Asset Bundles.

Use environment-specific credentials via CI secrets and Databricks service principals.
