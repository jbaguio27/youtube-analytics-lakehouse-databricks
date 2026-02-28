-- Bootstrap for Unity Catalog and Bronze raw tables.
-- Required metadata columns in each Bronze raw table:
-- snapshot_date, ingest_ts_utc, source_system, request_id, schema_version, run_id, payload

CREATE CATALOG IF NOT EXISTS youtube_analytics_dev;

CREATE SCHEMA IF NOT EXISTS youtube_analytics_dev.bronze;
CREATE SCHEMA IF NOT EXISTS youtube_analytics_dev.silver;
CREATE SCHEMA IF NOT EXISTS youtube_analytics_dev.gold;

CREATE CATALOG IF NOT EXISTS youtube_analytics;
CREATE SCHEMA IF NOT EXISTS youtube_analytics.bronze;
CREATE SCHEMA IF NOT EXISTS youtube_analytics.silver;
CREATE SCHEMA IF NOT EXISTS youtube_analytics.gold;

CREATE TABLE IF NOT EXISTS youtube_analytics_dev.bronze.channels_raw (
  snapshot_date DATE NOT NULL,
  ingest_ts_utc TIMESTAMP NOT NULL,
  source_system STRING NOT NULL,
  request_id STRING NOT NULL,
  schema_version STRING NOT NULL,
  run_id STRING NOT NULL,
  payload STRING NOT NULL
)
USING DELTA
PARTITIONED BY (snapshot_date);

CREATE TABLE IF NOT EXISTS youtube_analytics_dev.bronze.playlist_items_raw (
  snapshot_date DATE NOT NULL,
  ingest_ts_utc TIMESTAMP NOT NULL,
  source_system STRING NOT NULL,
  request_id STRING NOT NULL,
  schema_version STRING NOT NULL,
  run_id STRING NOT NULL,
  payload STRING NOT NULL
)
USING DELTA
PARTITIONED BY (snapshot_date);

CREATE TABLE IF NOT EXISTS youtube_analytics_dev.bronze.videos_raw (
  snapshot_date DATE NOT NULL,
  ingest_ts_utc TIMESTAMP NOT NULL,
  source_system STRING NOT NULL,
  request_id STRING NOT NULL,
  schema_version STRING NOT NULL,
  run_id STRING NOT NULL,
  payload STRING NOT NULL
)
USING DELTA
PARTITIONED BY (snapshot_date);

CREATE TABLE IF NOT EXISTS youtube_analytics_dev.bronze.analytics_channel_daily_raw (
  snapshot_date DATE NOT NULL,
  ingest_ts_utc TIMESTAMP NOT NULL,
  source_system STRING NOT NULL,
  request_id STRING NOT NULL,
  schema_version STRING NOT NULL,
  run_id STRING NOT NULL,
  payload STRING NOT NULL
)
USING DELTA
PARTITIONED BY (snapshot_date);

CREATE TABLE IF NOT EXISTS youtube_analytics_dev.bronze.analytics_video_daily_raw (
  snapshot_date DATE NOT NULL,
  ingest_ts_utc TIMESTAMP NOT NULL,
  source_system STRING NOT NULL,
  request_id STRING NOT NULL,
  schema_version STRING NOT NULL,
  run_id STRING NOT NULL,
  payload STRING NOT NULL
)
USING DELTA
PARTITIONED BY (snapshot_date);

CREATE TABLE IF NOT EXISTS youtube_analytics_dev.bronze.analytics_video_traffic_source_daily_raw (
  snapshot_date DATE NOT NULL,
  ingest_ts_utc TIMESTAMP NOT NULL,
  source_system STRING NOT NULL,
  request_id STRING NOT NULL,
  schema_version STRING NOT NULL,
  run_id STRING NOT NULL,
  payload STRING NOT NULL
)
USING DELTA
PARTITIONED BY (snapshot_date);

CREATE TABLE IF NOT EXISTS youtube_analytics_dev.bronze.analytics_video_country_daily_raw (
  snapshot_date DATE NOT NULL,
  ingest_ts_utc TIMESTAMP NOT NULL,
  source_system STRING NOT NULL,
  request_id STRING NOT NULL,
  schema_version STRING NOT NULL,
  run_id STRING NOT NULL,
  payload STRING NOT NULL
)
USING DELTA
PARTITIONED BY (snapshot_date);

CREATE TABLE IF NOT EXISTS youtube_analytics_dev.bronze.analytics_video_device_daily_raw (
  snapshot_date DATE NOT NULL,
  ingest_ts_utc TIMESTAMP NOT NULL,
  source_system STRING NOT NULL,
  request_id STRING NOT NULL,
  schema_version STRING NOT NULL,
  run_id STRING NOT NULL,
  payload STRING NOT NULL
)
USING DELTA
PARTITIONED BY (snapshot_date);

CREATE TABLE IF NOT EXISTS youtube_analytics.bronze.channels_raw (
  snapshot_date DATE NOT NULL,
  ingest_ts_utc TIMESTAMP NOT NULL,
  source_system STRING NOT NULL,
  request_id STRING NOT NULL,
  schema_version STRING NOT NULL,
  run_id STRING NOT NULL,
  payload STRING NOT NULL
)
USING DELTA
PARTITIONED BY (snapshot_date);

CREATE TABLE IF NOT EXISTS youtube_analytics.bronze.playlist_items_raw (
  snapshot_date DATE NOT NULL,
  ingest_ts_utc TIMESTAMP NOT NULL,
  source_system STRING NOT NULL,
  request_id STRING NOT NULL,
  schema_version STRING NOT NULL,
  run_id STRING NOT NULL,
  payload STRING NOT NULL
)
USING DELTA
PARTITIONED BY (snapshot_date);

CREATE TABLE IF NOT EXISTS youtube_analytics.bronze.videos_raw (
  snapshot_date DATE NOT NULL,
  ingest_ts_utc TIMESTAMP NOT NULL,
  source_system STRING NOT NULL,
  request_id STRING NOT NULL,
  schema_version STRING NOT NULL,
  run_id STRING NOT NULL,
  payload STRING NOT NULL
)
USING DELTA
PARTITIONED BY (snapshot_date);

CREATE TABLE IF NOT EXISTS youtube_analytics.bronze.analytics_channel_daily_raw (
  snapshot_date DATE NOT NULL,
  ingest_ts_utc TIMESTAMP NOT NULL,
  source_system STRING NOT NULL,
  request_id STRING NOT NULL,
  schema_version STRING NOT NULL,
  run_id STRING NOT NULL,
  payload STRING NOT NULL
)
USING DELTA
PARTITIONED BY (snapshot_date);

CREATE TABLE IF NOT EXISTS youtube_analytics.bronze.analytics_video_daily_raw (
  snapshot_date DATE NOT NULL,
  ingest_ts_utc TIMESTAMP NOT NULL,
  source_system STRING NOT NULL,
  request_id STRING NOT NULL,
  schema_version STRING NOT NULL,
  run_id STRING NOT NULL,
  payload STRING NOT NULL
)
USING DELTA
PARTITIONED BY (snapshot_date);

CREATE TABLE IF NOT EXISTS youtube_analytics.bronze.analytics_video_traffic_source_daily_raw (
  snapshot_date DATE NOT NULL,
  ingest_ts_utc TIMESTAMP NOT NULL,
  source_system STRING NOT NULL,
  request_id STRING NOT NULL,
  schema_version STRING NOT NULL,
  run_id STRING NOT NULL,
  payload STRING NOT NULL
)
USING DELTA
PARTITIONED BY (snapshot_date);

CREATE TABLE IF NOT EXISTS youtube_analytics.bronze.analytics_video_country_daily_raw (
  snapshot_date DATE NOT NULL,
  ingest_ts_utc TIMESTAMP NOT NULL,
  source_system STRING NOT NULL,
  request_id STRING NOT NULL,
  schema_version STRING NOT NULL,
  run_id STRING NOT NULL,
  payload STRING NOT NULL
)
USING DELTA
PARTITIONED BY (snapshot_date);

CREATE TABLE IF NOT EXISTS youtube_analytics.bronze.analytics_video_device_daily_raw (
  snapshot_date DATE NOT NULL,
  ingest_ts_utc TIMESTAMP NOT NULL,
  source_system STRING NOT NULL,
  request_id STRING NOT NULL,
  schema_version STRING NOT NULL,
  run_id STRING NOT NULL,
  payload STRING NOT NULL
)
USING DELTA
PARTITIONED BY (snapshot_date);
