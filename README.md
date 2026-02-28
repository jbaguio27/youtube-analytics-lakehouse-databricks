# YouTube Analytics Lakehouse
A Databricks-first ELT project that ingests YouTube API data into Bronze, transforms to Silver with Lakeflow SQL, and builds Gold marts with dbt.

<img width="2511" height="1146" alt="image" src="https://github.com/user-attachments/assets/221990d7-1977-41e4-82ea-12991d41d90c" />

## What this Repo Contains?
- Databricks Asset Bundle configuration for one job and one Lakeflow pipeline (`databricks.yml`, `bundles/bundle.yml`).
- Python ingestion and operations tasks for Bronze ingestion, optimization, and run logging (`ingestion/tasks/*.py`).
- Lakeflow SQL definitions for Silver materialized views and country reference mapping (`lakeflow/bronze_to_silver_pipeline.sql`, `lakeflow/country_reference.sql`).
- dbt project with Gold models and tests (`dbt/dbt_project.yml`, `dbt/models`, `dbt/tests`, `dbt/profiles.yml`).
- Utility scripts for OAuth refresh token retrieval, Databricks secret bootstrap, Unity Catalog/Bronze validation, and post-deploy smoke checks (`scripts/*.py`).

## End to End Process
1. Run one-time local OAuth flow to retrieve a YouTube refresh token (`scripts/get_youtube_refresh_token.py`).
2. Write YouTube OAuth credentials into a Databricks secret scope (`scripts/bootstrap_youtube_secrets.py`).
3. Bootstrap Unity Catalog schemas/Bronze tables and validate Bronze metadata contract (`scripts/unity_catalog_setup.py`, `lakeflow/bootstrap_unity_catalog.sql`).
4. Deploy/run the Databricks bundle job (`databricks.yml`) with tasks:
   - `init_run_context`
   - `ingest_data_api_to_bronze`
   - `ingest_analytics_api_to_bronze`
   - `run_lakeflow_pipeline`
   - `dbt_run_gold`
   - `dbt_test`
   - `optimize_tables`
   - `post_deploy_smoke_checks`
   - `finalize_run_log`
5. Build Gold models from Silver sources in schema `silver` and run post-deploy health checks (`dbt/models/schema.yml`, `scripts/post_deploy_smoke_checks.py`).

## Architecture
- Orchestration: Databricks Job + Databricks Asset Bundles (`databricks.yml`, `bundles/bundle.yml`).
- Ingestion: Python Spark tasks ingest raw YouTube API payloads to Bronze Delta tables (`ingestion/tasks/ingest_data_api_to_bronze.py`, `ingestion/tasks/ingest_analytics_api_to_bronze.py`).
- Transformation: Lakeflow Declarative Pipeline SQL creates Silver materialized views (`lakeflow/bronze_to_silver_pipeline.sql`).
- Table maintenance: `optimize_tables.py` runs `OPTIMIZE` on supported Delta tables and skips views/materialized views by design.
- Gold modeling/testing: dbt models and tests run from Databricks tasks (`ingestion/tasks/dbt_run_gold.py`, `ingestion/tasks/dbt_test.py`, `dbt/models`, `dbt/tests`).
- CI validation: GitHub Actions workflow (`.github/workflows/ci.yml`).
- Dev deploy workflow (`.github/workflows/dev-deploy.yml`) for validate/deploy/run/smoke on `main` or manual trigger.
- Prod release workflow (`.github/workflows/prod-release.yml`).

## Tech Stack
- Python 3.11+ and uv.
  - Where in repo: `pyproject.toml`, `uv.lock`.
- Databricks SDK for Python.
  - Where in repo: `pyproject.toml`, `ingestion/tasks/finalize_run_log.py`.
- Databricks CLI (external prerequisite used by scripts/commands).
  - Where in repo: `scripts/bootstrap_youtube_secrets.py`, `scripts/unity_catalog_setup.py`, `databricks.yml` usage.
- Spark / PySpark runtime.
  - Where in repo: `ingestion/tasks/*.py` imports `pyspark.sql`.
- Lakeflow Declarative Pipelines SQL.
  - Where in repo: `lakeflow/bronze_to_silver_pipeline.sql`, `databricks.yml` pipeline resource.
- dbt with `dbt-databricks` adapter.
  - Where in repo: `dbt/dbt_project.yml`, `dbt/profiles.yml`, `pyproject.toml`.
- GitHub Actions.
  - Where in repo: `.github/workflows/ci.yml`.

## Data Sources
- YouTube Data API v3 resources:
  - `channels`
  - `playlistItems`
  - `videos`
  - Evidence: `ingestion/tasks/ingest_data_api_to_bronze.py`.
- YouTube Analytics API reports endpoint:
  - `https://youtubeanalytics.googleapis.com/v2/reports`
  - Evidence: `ingestion/tasks/ingest_analytics_api_to_bronze.py`.
- Google OAuth endpoints for token flow:
  - Evidence: `scripts/get_youtube_refresh_token.py`.

## Data Model
Bronze tables:
- `channels_raw`
- `playlist_items_raw`
- `videos_raw`
- `analytics_channel_daily_raw`
- `analytics_video_daily_raw`
- `analytics_video_traffic_source_daily_raw`
- `analytics_video_country_daily_raw`
- `analytics_video_device_daily_raw`
- `run_context_log`
- Evidence: `lakeflow/bootstrap_unity_catalog.sql`, `ingestion/tasks/init_run_context.py`.

Silver materialized views (`silver`):
- `silver_channels`
- `silver_videos`
- `silver_video_stats_snapshot`
- `silver_video_metadata_scd2`
- `fact_channel_daily_metrics`
- `fact_video_daily_metrics`
- `fact_video_traffic_source_metrics`
- `fact_video_country_metrics`
- `fact_video_device_metrics`
- `dim_date`
- `dim_traffic_source`
- `dim_country`
- `dim_device`
- `dim_country_reference`
- Evidence: `lakeflow/bronze_to_silver_pipeline.sql`, `lakeflow/country_reference.sql`.

Gold dbt models:
- `gold_channel_daily_summary`
- `gold_video_daily_summary`
- `gold_video_country_daily_summary`
- `gold_video_device_daily_summary`
- `gold_video_traffic_source_daily_summary`
- Evidence: `dbt/models/*.sql`.

## Pipelines and Orchestration
- Databricks pipeline resource:
  - `youtube_analytics_lakeflow` (triggered/non-continuous, serverless).
  - Evidence: `databricks.yml`, `bundles/bundle.yml`.
- Databricks job resource:
  - `youtube_analytics_job` with explicit task dependencies and performance target.
  - Evidence: `databricks.yml`, `bundles/bundle.yml`.
- Finalization behavior:
  - `finalize_run_log` runs with `run_if: ALL_DONE` and writes terminal status.
  - Evidence: `databricks.yml`, `ingestion/tasks/finalize_run_log.py`.
- CI pipeline:
  - `uv sync`, `dbt parse`, optional Databricks bundle validate and dbt singular tests when secrets are present.
  - Evidence: `.github/workflows/ci.yml`.

## Local Setup
Prerequisites:
- Python 3.11+.
- `uv` installed.
- Databricks CLI installed and authenticated (`databricks auth login ...`).

Setup commands:
```bash
uv venv .venv
uv sync
```

Create local env file from template:
```bash
cp .env.example .env
```

Secrets template for local dbt runtime:
```env
DBT_DATABRICKS_HOST=https://<your-databricks-host>
DBT_DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/<your-warehouse-id>
DBT_DATABRICKS_TOKEN=<your-token>
```

## 10) Runbook
### How to run ingestion
OAuth + secret bootstrap:
```bash
uv run python scripts/get_youtube_refresh_token.py
uv run python scripts/bootstrap_youtube_secrets.py --profile <your_profile> --scope youtube-analytics
```

Unity Catalog/Bronze bootstrap + validation:
```bash
uv run python scripts/unity_catalog_setup.py --warehouse-id <your_sql_warehouse_id> --action all
```

Run full Databricks job:
```bash
databricks bundle validate --target dev --profile <your_profile> --var "alert_email=<your_email>"
databricks bundle deploy --target dev --profile <your_profile> --var "alert_email=<your_email>"
databricks bundle run youtube_analytics_job --target dev --profile <your_profile> --var "alert_email=<your_email>"
```

### Production promotion
Prechecks:
- Databricks profile for prod is configured and authenticated.
- Secret scope `youtube-analytics` contains required keys used by runtime:
  - `yt_client_id`
  - `yt_client_secret`
  - `yt_refresh_token`
  - `dbt_host`
  - `dbt_http_path`
  - `dbt_token`
- The principal used by prod profile has:
  - access to Unity Catalog `youtube_analytics`
  - permission to run the configured SQL Warehouse (for dbt tasks)

Promotion Commands:
```bash
databricks bundle validate --target prod --profile <your_prod_profile> --var "alert_email=<your_email>"
databricks bundle deploy --target prod --profile <your_prod_profile> --var "alert_email=<your_email>"
databricks bundle run youtube_analytics_job --target prod --profile <your_prod_profile> --var "alert_email=<your_email>"
```

Post-deploy Checks:
- Run smoke checks:
```bash
uv run python scripts/post_deploy_smoke_checks.py --warehouse-id <your_sql_warehouse_id> --profile <your_prod_profile> --catalog youtube_analytics
```
- Optional custom freshness threshold (match your prod-release workflow input `max_gold_lag_days`):
```bash
uv run python scripts/post_deploy_smoke_checks.py --warehouse-id <your_sql_warehouse_id> --profile <your_prod_profile> --catalog youtube_analytics --max-gold-lag-days <max_lag_days>
```

Rollback:
- Redeploy the last known-good git tag/commit, then run the same prod deployment commands.
- Example:
```bash
git checkout <last_known_good_tag_or_commit>
databricks bundle validate --target prod --profile <your_prod_profile> --var "alert_email=<your_email>"
databricks bundle deploy --target prod --profile <your_prod_profile> --var "alert_email=<your_email>"
databricks bundle run youtube_analytics_job --target prod --profile <your_prod_profile> --var "alert_email=<your_email>"
uv run python scripts/post_deploy_smoke_checks.py --warehouse-id <your_sql_warehouse_id> --profile <your_prod_profile> --catalog youtube_analytics --max-gold-lag-days <max_lag_days>
```

### How to Run Transforms
Lakeflow transforms run inside the Databricks job (`run_lakeflow_pipeline`).

Run dbt transforms directly:
```bash
uv run dbt run --project-dir dbt --profiles-dir dbt --target dev --select path:models
```

### How to Run Tests
Run all dbt tests:
```bash
uv run dbt test --project-dir dbt --profiles-dir dbt --target dev
```

Run singular tests only:
```bash
uv run dbt test --project-dir dbt --profiles-dir dbt --target dev --select test_type:singular
```

### How to Build Docs
Generate dbt docs artifacts:
```bash
uv run dbt docs generate --project-dir dbt --profiles-dir dbt --target dev
```

Docs Hosting/Publishing is Not in repo yet.
Evidence needed: a docs deploy workflow or hosting config.

## Data Quality and Testing
Implemented checks:
- Gold uniqueness tests by grain (`dbt/tests/test_gold_*_unique.sql`).
- Non-negative metric test (`dbt/tests/test_gold_metrics_non_negative.sql`).
- Gold recency/freshness test (`dbt/tests/test_gold_freshness_recency.sql`).
- `not_null` and relationship checks in `dbt/models/schema.yml`.
- Warning-only monitor for new traffic source IDs (`dbt/tests/warn_new_traffic_source_ids.sql`).

## Observability and Logging
- `init_run_context` writes run metadata and context JSON to `bronze.run_context_log`.
  - Evidence: `ingestion/tasks/init_run_context.py`.
- `finalize_run_log` updates run status/finalization fields (`run_status`, `finished_ts_utc`, `finalized_ts_utc`, `finalize_task_run_id`).
  - Evidence: `ingestion/tasks/finalize_run_log.py`.
- Ingestion tasks print structured JSON summaries with row/table counts.
  - Evidence: `ingestion/tasks/ingest_data_api_to_bronze.py`, `ingestion/tasks/ingest_analytics_api_to_bronze.py`.

## Repository Structure
```text
.
├── .env.example
├── .github/
│   └── workflows/
│       └── ci.yml
│       └── dev-deploy.yml
│       └── prod-release.yml
├── bundles/
│   └── bundle.yml
├── databricks.yml
├── docs/
│   └── .gitkeep
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── gold_channel_daily_summary.sql
│   │   ├── gold_video_country_daily_summary.sql
│   │   ├── gold_video_daily_summary.sql
│   │   ├── gold_video_device_daily_summary.sql
│   │   ├── gold_video_traffic_source_daily_summary.sql
│   │   └── schema.yml
│   └── tests/
│       ├── test_gold_channel_daily_summary_unique.sql
│       ├── test_gold_freshness_recency.sql
│       ├── test_gold_metrics_non_negative.sql
│       ├── test_gold_video_country_daily_summary_unique.sql
│       ├── test_gold_video_daily_summary_unique.sql
│       ├── test_gold_video_device_daily_summary_unique.sql
│       ├── test_gold_video_traffic_source_daily_summary_unique.sql
│       └── warn_new_traffic_source_ids.sql
├── ingestion/
│   └── tasks/
│       ├── dbt_run_gold.py
│       ├── dbt_test.py
│       ├── finalize_run_log.py
│       ├── ingest_analytics_api_to_bronze.py
│       ├── ingest_data_api_to_bronze.py
│       ├── init_run_context.py
│       └── optimize_tables.py
├── lakeflow/
│   ├── bootstrap_unity_catalog.sql
│   ├── bronze_to_silver_pipeline.sql
│   └── country_reference.sql
├── pyproject.toml
├── README.md
├── requirements.txt
├── scripts/
│   ├── bootstrap_youtube_secrets.py
│   ├── get_youtube_refresh_token.py
│   ├── post_deploy_smoke_checks.py
│   └── unity_catalog_setup.py
├── tests/
│   ├── conftest.py
│   ├── test_ingest_analytics_window.py
│   └── sql/
└── uv.lock
```

## Troubleshooting
- dbt cannot connect (`Env var required but not provided`):
  - Set `DBT_DATABRICKS_HOST`, `DBT_DATABRICKS_HTTP_PATH`, `DBT_DATABRICKS_TOKEN`.
  - Evidence: `dbt/profiles.yml`, `.env.example`.
- Bundle validation/deploy fails due CLI auth/profile:
  - Verify profile with `databricks auth env --profile <your_profile>`.
  - Evidence: repository commands/scripts rely on Databricks CLI.
- dbt job task cannot find dbt project:
  - Confirm bundle file sync path and `DBT_PROJECT_DIR` value.
  - Evidence: project-dir search logic in `ingestion/tasks/dbt_run_gold.py` and `ingestion/tasks/dbt_test.py`.
- Optimize step skips Silver views:
  - Expected behavior for unsupported table types (`VIEW`, `MATERIALIZED_VIEW`); `OPTIMIZE` is applied only to supported Delta tables.
  - Evidence: `ingestion/tasks/optimize_tables.py`.

## Roadmap
- Add dashboard/reporting assets.
  - Not in repo yet.
- Add automated local/integration tests for Spark task modules with `dbutils` mocking.
  - Not in repo yet.

## Documentation and Images
### Databricks Lakeflow Declarative Pipeline Dev and Prod

<img width="591" height="838" alt="image" src="https://github.com/user-attachments/assets/9b80bb0b-4d12-4990-be3c-77f580f78597" />

### Databricks Task Dependency Dev and Prod

<img width="2373" height="1072" alt="image" src="https://github.com/user-attachments/assets/ec6fe6dc-ae05-42d0-9ba3-fa8a086ea38e" />

### Database Model Design

<img width="480" height="1314" alt="image" src="https://github.com/user-attachments/assets/d637eb9b-0b06-49a7-bae0-685bb08883eb" />

### DBT Lineage

<img width="1019" height="1090" alt="image" src="https://github.com/user-attachments/assets/e050c7dc-82ec-4725-8154-cdbede2c9fa0" />

## Contributing
- Create a branch and keep changes scoped.
- Run baseline checks before PR:
  - `uv sync --frozen`
  - `uv run dbt parse --project-dir dbt --profiles-dir dbt --target dev`
- If CI secrets are configured, ensure optional Databricks/dbt integration checks pass.
- Prod release workflow secrets:
  - `DATABRICKS_HOST`
  - `DATABRICKS_TOKEN`
  - `DATABRICKS_SQL_WAREHOUSE_ID`
  - `ALERT_EMAIL`
- Dev deploy workflow uses the same secrets:
  - `DATABRICKS_HOST`
  - `DATABRICKS_TOKEN`
  - `DATABRICKS_SQL_WAREHOUSE_ID`
  - `ALERT_EMAIL`
