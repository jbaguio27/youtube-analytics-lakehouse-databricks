-- Bronze -> Silver transformations for the first Silver increment.
-- Keys and idempotency notes:
-- 1) silver_channels dedup key: channel_id
-- 2) silver_video_stats_snapshot dedup key: (video_id, fetched_at_utc)
-- 3) silver_video_metadata_scd2 business key: video_id, version window key: valid_from_utc
-- 4) silver_videos dedup key: video_id and current FK resolution to SCD2 row

CREATE OR REFRESH MATERIALIZED VIEW silver.silver_channels
AS
WITH parsed_channels AS (
  SELECT
    r.snapshot_date,
    r.ingest_ts_utc,
    r.request_id,
    r.run_id,
    r.schema_version,
    explode_outer(
      from_json(
        r.payload,
        'STRUCT<items: ARRAY<STRUCT<id: STRING, snippet: STRUCT<title: STRING, description: STRING, customUrl: STRING, country: STRING, publishedAt: STRING>, statistics: STRUCT<viewCount: STRING, subscriberCount: STRING, hiddenSubscriberCount: BOOLEAN, videoCount: STRING>>>>'
      ).items
    ) AS item
  FROM bronze.channels_raw AS r
),
typed_channels AS (
  SELECT
    item.id AS channel_id,
    item.snippet.title AS channel_title,
    item.snippet.description AS channel_description,
    item.snippet.customUrl AS custom_url,
    item.snippet.country AS channel_country_code,
    to_timestamp(item.snippet.publishedAt) AS channel_published_at_utc,
    CAST(item.statistics.viewCount AS BIGINT) AS channel_view_count,
    CAST(item.statistics.subscriberCount AS BIGINT) AS channel_subscriber_count,
    item.statistics.hiddenSubscriberCount AS hidden_subscriber_count,
    CAST(item.statistics.videoCount AS BIGINT) AS channel_video_count,
    snapshot_date,
    ingest_ts_utc,
    request_id,
    run_id,
    schema_version
  FROM parsed_channels
  WHERE item.id IS NOT NULL
),
latest_channels AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY channel_id
      ORDER BY snapshot_date DESC, ingest_ts_utc DESC, request_id DESC
    ) AS rn
  FROM typed_channels
)
SELECT
  channel_id,
  channel_title,
  channel_description,
  custom_url,
  channel_country_code,
  channel_published_at_utc,
  channel_view_count,
  channel_subscriber_count,
  hidden_subscriber_count,
  channel_video_count,
  snapshot_date,
  ingest_ts_utc,
  request_id,
  run_id,
  schema_version
FROM latest_channels
WHERE rn = 1;

CREATE OR REFRESH MATERIALIZED VIEW silver.silver_video_stats_snapshot
AS
WITH parsed_video_stats AS (
  SELECT
    r.snapshot_date,
    r.ingest_ts_utc,
    r.request_id,
    r.run_id,
    r.schema_version,
    explode_outer(
      from_json(
        r.payload,
        'STRUCT<items: ARRAY<STRUCT<id: STRING, snippet: STRUCT<channelId: STRING>, statistics: STRUCT<viewCount: STRING, likeCount: STRING, favoriteCount: STRING, commentCount: STRING>>>>'
      ).items
    ) AS item
  FROM bronze.videos_raw AS r
),
typed_video_stats AS (
  SELECT
    item.id AS video_id,
    item.snippet.channelId AS channel_id,
    ingest_ts_utc AS fetched_at_utc,
    snapshot_date,
    CAST(item.statistics.viewCount AS BIGINT) AS view_count,
    CAST(item.statistics.likeCount AS BIGINT) AS like_count,
    CAST(item.statistics.favoriteCount AS BIGINT) AS favorite_count,
    CAST(item.statistics.commentCount AS BIGINT) AS comment_count,
    ingest_ts_utc,
    request_id,
    run_id,
    schema_version
  FROM parsed_video_stats
  WHERE item.id IS NOT NULL
),
dedup_video_stats AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY video_id, fetched_at_utc
      ORDER BY request_id DESC
    ) AS rn
  FROM typed_video_stats
)
SELECT
  video_id,
  channel_id,
  fetched_at_utc,
  snapshot_date,
  view_count,
  like_count,
  favorite_count,
  comment_count,
  ingest_ts_utc,
  request_id,
  run_id,
  schema_version
FROM dedup_video_stats
WHERE rn = 1;

CREATE OR REFRESH MATERIALIZED VIEW silver.silver_video_metadata_scd2
AS
WITH parsed_video_metadata AS (
  SELECT
    r.snapshot_date,
    r.ingest_ts_utc,
    r.request_id,
    r.run_id,
    r.schema_version,
    explode_outer(
      from_json(
        r.payload,
        'STRUCT<items: ARRAY<STRUCT<id: STRING, snippet: STRUCT<channelId: STRING, title: STRING, description: STRING, publishedAt: STRING, defaultLanguage: STRING, defaultAudioLanguage: STRING>, contentDetails: STRUCT<duration: STRING, dimension: STRING, definition: STRING, caption: STRING, licensedContent: BOOLEAN, projection: STRING>, status: STRUCT<uploadStatus: STRING, privacyStatus: STRING, embeddable: BOOLEAN, publicStatsViewable: BOOLEAN, madeForKids: BOOLEAN, selfDeclaredMadeForKids: BOOLEAN>, topicDetails: STRUCT<topicCategories: ARRAY<STRING>>>>>'
      ).items
    ) AS item
  FROM bronze.videos_raw AS r
),
typed_video_metadata AS (
  SELECT
    item.id AS video_id,
    item.snippet.channelId AS channel_id,
    item.snippet.title AS video_title,
    item.snippet.description AS video_description,
    to_timestamp(item.snippet.publishedAt) AS video_published_at_utc,
    item.snippet.defaultLanguage AS default_language,
    item.snippet.defaultAudioLanguage AS default_audio_language,
    item.contentDetails.duration AS duration_iso8601,
    item.contentDetails.dimension AS video_dimension,
    item.contentDetails.definition AS video_definition,
    item.contentDetails.caption AS caption_status,
    item.contentDetails.licensedContent AS licensed_content,
    item.contentDetails.projection AS projection_type,
    item.status.uploadStatus AS upload_status,
    item.status.privacyStatus AS privacy_status,
    item.status.embeddable AS embeddable,
    item.status.publicStatsViewable AS public_stats_viewable,
    item.status.madeForKids AS made_for_kids,
    item.status.selfDeclaredMadeForKids AS self_declared_made_for_kids,
    concat_ws('|', item.topicDetails.topicCategories) AS topic_categories_csv,
    ingest_ts_utc AS observed_at_utc,
    snapshot_date,
    ingest_ts_utc,
    request_id,
    run_id,
    schema_version
  FROM parsed_video_metadata
  WHERE item.id IS NOT NULL
),
hashed_video_metadata AS (
  SELECT
    *,
    sha2(
      concat_ws(
        '||',
        coalesce(channel_id, ''),
        coalesce(video_title, ''),
        coalesce(video_description, ''),
        coalesce(CAST(video_published_at_utc AS STRING), ''),
        coalesce(default_language, ''),
        coalesce(default_audio_language, ''),
        coalesce(duration_iso8601, ''),
        coalesce(video_dimension, ''),
        coalesce(video_definition, ''),
        coalesce(caption_status, ''),
        coalesce(CAST(licensed_content AS STRING), ''),
        coalesce(projection_type, ''),
        coalesce(upload_status, ''),
        coalesce(privacy_status, ''),
        coalesce(CAST(embeddable AS STRING), ''),
        coalesce(CAST(public_stats_viewable AS STRING), ''),
        coalesce(CAST(made_for_kids AS STRING), ''),
        coalesce(CAST(self_declared_made_for_kids AS STRING), ''),
        coalesce(topic_categories_csv, '')
      ),
      256
    ) AS metadata_hash
  FROM typed_video_metadata
),
ordered_video_metadata AS (
  SELECT
    *,
    lag(metadata_hash) OVER (
      PARTITION BY video_id
      ORDER BY observed_at_utc ASC, request_id ASC
    ) AS previous_metadata_hash
  FROM hashed_video_metadata
),
version_start_rows AS (
  SELECT
    *
  FROM ordered_video_metadata
  WHERE previous_metadata_hash IS NULL OR previous_metadata_hash <> metadata_hash
),
windowed_scd2 AS (
  SELECT
    video_id,
    channel_id,
    video_title,
    video_description,
    video_published_at_utc,
    default_language,
    default_audio_language,
    duration_iso8601,
    video_dimension,
    video_definition,
    caption_status,
    licensed_content,
    projection_type,
    upload_status,
    privacy_status,
    embeddable,
    public_stats_viewable,
    made_for_kids,
    self_declared_made_for_kids,
    topic_categories_csv,
    metadata_hash,
    observed_at_utc AS valid_from_utc,
    lead(observed_at_utc) OVER (
      PARTITION BY video_id
      ORDER BY observed_at_utc ASC, request_id ASC
    ) AS next_valid_from_utc,
    snapshot_date,
    ingest_ts_utc,
    request_id,
    run_id,
    schema_version
  FROM version_start_rows
)
SELECT
  sha2(concat_ws('||', video_id, CAST(valid_from_utc AS STRING)), 256) AS video_meta_sk,
  video_id,
  channel_id,
  video_title,
  video_description,
  video_published_at_utc,
  default_language,
  default_audio_language,
  duration_iso8601,
  video_dimension,
  video_definition,
  caption_status,
  licensed_content,
  projection_type,
  upload_status,
  privacy_status,
  embeddable,
  public_stats_viewable,
  made_for_kids,
  self_declared_made_for_kids,
  topic_categories_csv,
  metadata_hash,
  valid_from_utc,
  CASE
    WHEN next_valid_from_utc IS NULL THEN TIMESTAMP '9999-12-31 23:59:59.999999'
    ELSE next_valid_from_utc - INTERVAL 1 MICROSECOND
  END AS valid_to_utc,
  CASE
    WHEN next_valid_from_utc IS NULL THEN TRUE
    ELSE FALSE
  END AS is_current,
  snapshot_date,
  ingest_ts_utc,
  request_id,
  run_id,
  schema_version
FROM windowed_scd2;

CREATE OR REFRESH MATERIALIZED VIEW silver.silver_videos
AS
WITH parsed_videos AS (
  SELECT
    r.snapshot_date,
    r.ingest_ts_utc,
    r.request_id,
    r.run_id,
    r.schema_version,
    explode_outer(
      from_json(
        r.payload,
        'STRUCT<items: ARRAY<STRUCT<id: STRING, snippet: STRUCT<channelId: STRING, title: STRING, publishedAt: STRING>, status: STRUCT<privacyStatus: STRING, uploadStatus: STRING>>>>'
      ).items
    ) AS item
  FROM bronze.videos_raw AS r
),
typed_videos AS (
  SELECT
    item.id AS video_id,
    item.snippet.channelId AS channel_id,
    item.snippet.title AS latest_video_title,
    to_timestamp(item.snippet.publishedAt) AS video_published_at_utc,
    item.status.privacyStatus AS latest_privacy_status,
    item.status.uploadStatus AS latest_upload_status,
    snapshot_date,
    ingest_ts_utc,
    request_id,
    run_id,
    schema_version
  FROM parsed_videos
  WHERE item.id IS NOT NULL
),
latest_videos AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY video_id
      ORDER BY snapshot_date DESC, ingest_ts_utc DESC, request_id DESC
    ) AS rn
  FROM typed_videos
),
current_video_metadata AS (
  SELECT
    video_id,
    video_meta_sk
  FROM silver.silver_video_metadata_scd2
  WHERE is_current = TRUE
)
SELECT
  v.video_id,
  v.channel_id,
  m.video_meta_sk AS current_video_meta_sk,
  v.latest_video_title,
  v.video_published_at_utc,
  v.latest_privacy_status,
  v.latest_upload_status,
  v.snapshot_date,
  v.ingest_ts_utc,
  v.request_id,
  v.run_id,
  v.schema_version
FROM latest_videos AS v
LEFT JOIN current_video_metadata AS m
  ON v.video_id = m.video_id
WHERE v.rn = 1;

CREATE OR REFRESH MATERIALIZED VIEW silver.fact_channel_daily_metrics
AS
WITH current_channel AS (
  SELECT channel_id
  FROM silver.silver_channels
  ORDER BY ingest_ts_utc DESC, request_id DESC
  LIMIT 1
),
parsed_channel_payload AS (
  SELECT
    r.snapshot_date,
    r.ingest_ts_utc,
    r.request_id,
    r.run_id,
    r.schema_version,
    from_json(
      r.payload,
      'STRUCT<columnHeaders: ARRAY<STRUCT<name: STRING, columnType: STRING, dataType: STRING>>, rows: ARRAY<ARRAY<STRING>>>',
      map('primitivesAsString', 'true')
    ) AS parsed
  FROM bronze.analytics_channel_daily_raw AS r
),
exploded_channel_rows AS (
  SELECT
    c.channel_id,
    p.snapshot_date,
    p.ingest_ts_utc,
    p.request_id,
    p.run_id,
    p.schema_version,
    transform(p.parsed.columnHeaders, x -> x.name) AS header_names,
    explode_outer(p.parsed.rows) AS row_values
  FROM parsed_channel_payload AS p
  CROSS JOIN current_channel AS c
),
typed_channel_rows AS (
  SELECT
    channel_id,
    to_date(element_at(row_values, CAST(array_position(header_names, 'day') AS INT))) AS date,
    CAST(element_at(row_values, CAST(array_position(header_names, 'views') AS INT)) AS BIGINT) AS views,
    CAST(element_at(row_values, CAST(array_position(header_names, 'likes') AS INT)) AS BIGINT) AS likes,
    CAST(element_at(row_values, CAST(array_position(header_names, 'comments') AS INT)) AS BIGINT) AS comments,
    CAST(element_at(row_values, CAST(array_position(header_names, 'estimatedMinutesWatched') AS INT)) AS BIGINT) AS estimated_minutes_watched,
    CAST(element_at(row_values, CAST(array_position(header_names, 'subscribersGained') AS INT)) AS BIGINT) AS subscribers_gained,
    CAST(element_at(row_values, CAST(array_position(header_names, 'subscribersLost') AS INT)) AS BIGINT) AS subscribers_lost,
    snapshot_date,
    ingest_ts_utc,
    request_id,
    run_id,
    schema_version
  FROM exploded_channel_rows
),
dedup_channel_daily AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY channel_id, date
      ORDER BY snapshot_date DESC, ingest_ts_utc DESC, request_id DESC
    ) AS rn
  FROM typed_channel_rows
  WHERE channel_id IS NOT NULL AND date IS NOT NULL
)
SELECT
  channel_id,
  date,
  views,
  likes,
  comments,
  estimated_minutes_watched,
  subscribers_gained,
  subscribers_lost,
  snapshot_date,
  ingest_ts_utc,
  request_id,
  run_id,
  schema_version
FROM dedup_channel_daily
WHERE rn = 1;

CREATE OR REFRESH MATERIALIZED VIEW silver.fact_video_daily_metrics
AS
WITH parsed_video_payload AS (
  SELECT
    r.snapshot_date,
    r.ingest_ts_utc,
    r.request_id,
    r.run_id,
    r.schema_version,
    from_json(
      r.payload,
      'STRUCT<columnHeaders: ARRAY<STRUCT<name: STRING, columnType: STRING, dataType: STRING>>, rows: ARRAY<ARRAY<STRING>>>',
      map('primitivesAsString', 'true')
    ) AS parsed
  FROM bronze.analytics_video_daily_raw AS r
),
exploded_video_rows AS (
  SELECT
    p.snapshot_date,
    p.ingest_ts_utc,
    p.request_id,
    p.run_id,
    p.schema_version,
    transform(p.parsed.columnHeaders, x -> x.name) AS header_names,
    explode_outer(p.parsed.rows) AS row_values
  FROM parsed_video_payload AS p
),
typed_video_rows AS (
  SELECT
    element_at(row_values, CAST(array_position(header_names, 'video') AS INT)) AS video_id,
    to_date(element_at(row_values, CAST(array_position(header_names, 'day') AS INT))) AS date,
    CAST(element_at(row_values, CAST(array_position(header_names, 'views') AS INT)) AS BIGINT) AS views,
    CAST(element_at(row_values, CAST(array_position(header_names, 'likes') AS INT)) AS BIGINT) AS likes,
    CAST(element_at(row_values, CAST(array_position(header_names, 'comments') AS INT)) AS BIGINT) AS comments,
    CAST(element_at(row_values, CAST(array_position(header_names, 'estimatedMinutesWatched') AS INT)) AS BIGINT) AS estimated_minutes_watched,
    CAST(element_at(row_values, CAST(array_position(header_names, 'averageViewDuration') AS INT)) AS DOUBLE) AS average_view_duration_seconds,
    snapshot_date,
    ingest_ts_utc,
    request_id,
    run_id,
    schema_version
  FROM exploded_video_rows
),
dedup_video_daily AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY video_id, date
      ORDER BY snapshot_date DESC, ingest_ts_utc DESC, request_id DESC
    ) AS rn
  FROM typed_video_rows
  WHERE video_id IS NOT NULL AND date IS NOT NULL
)
SELECT
  video_id,
  date,
  views,
  likes,
  comments,
  estimated_minutes_watched,
  average_view_duration_seconds,
  snapshot_date,
  ingest_ts_utc,
  request_id,
  run_id,
  schema_version
FROM dedup_video_daily
WHERE rn = 1;

CREATE OR REFRESH MATERIALIZED VIEW silver.fact_video_traffic_source_metrics
AS
WITH parsed_traffic_payload AS (
  SELECT
    r.snapshot_date,
    r.ingest_ts_utc,
    r.request_id,
    r.run_id,
    r.schema_version,
    from_json(
      r.payload,
      'STRUCT<columnHeaders: ARRAY<STRUCT<name: STRING, columnType: STRING, dataType: STRING>>, rows: ARRAY<ARRAY<STRING>>>',
      map('primitivesAsString', 'true')
    ) AS parsed
  FROM bronze.analytics_video_traffic_source_daily_raw AS r
),
exploded_traffic_rows AS (
  SELECT
    p.snapshot_date,
    p.ingest_ts_utc,
    p.request_id,
    p.run_id,
    p.schema_version,
    transform(p.parsed.columnHeaders, x -> x.name) AS header_names,
    explode_outer(p.parsed.rows) AS row_values
  FROM parsed_traffic_payload AS p
),
typed_traffic_rows AS (
  SELECT
    CASE
      WHEN array_position(header_names, 'video') > 0
        THEN element_at(row_values, CAST(array_position(header_names, 'video') AS INT))
      ELSE NULL
    END AS video_id,
    CASE
      WHEN array_position(header_names, 'day') > 0
        THEN COALESCE(
          to_date(element_at(row_values, CAST(array_position(header_names, 'day') AS INT))),
          snapshot_date
        )
      ELSE snapshot_date
    END AS date,
    upper(element_at(row_values, CAST(array_position(header_names, 'insightTrafficSourceType') AS INT))) AS source_id,
    CAST(
      CASE
        WHEN array_position(header_names, 'views') > 0
          THEN element_at(row_values, CAST(array_position(header_names, 'views') AS INT))
        ELSE NULL
      END AS BIGINT
    ) AS views,
    CAST(
      CASE
        WHEN array_position(header_names, 'estimatedMinutesWatched') > 0
          THEN element_at(row_values, CAST(array_position(header_names, 'estimatedMinutesWatched') AS INT))
        ELSE NULL
      END AS BIGINT
    ) AS estimated_minutes_watched,
    snapshot_date,
    ingest_ts_utc,
    request_id,
    run_id,
    schema_version
  FROM exploded_traffic_rows
),
dedup_traffic_metrics AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY video_id, date, source_id
      ORDER BY snapshot_date DESC, ingest_ts_utc DESC, request_id DESC
    ) AS rn
  FROM typed_traffic_rows
  WHERE video_id IS NOT NULL
    AND date IS NOT NULL
    AND source_id IS NOT NULL
    AND source_id <> ''
)
SELECT
  video_id,
  date,
  source_id,
  views,
  estimated_minutes_watched,
  snapshot_date,
  ingest_ts_utc,
  request_id,
  run_id,
  schema_version
FROM dedup_traffic_metrics
WHERE rn = 1;

CREATE OR REFRESH MATERIALIZED VIEW silver.fact_video_country_metrics
AS
WITH parsed_country_payload AS (
  SELECT
    r.snapshot_date,
    r.ingest_ts_utc,
    r.request_id,
    r.run_id,
    r.schema_version,
    from_json(
      r.payload,
      'STRUCT<columnHeaders: ARRAY<STRUCT<name: STRING, columnType: STRING, dataType: STRING>>, rows: ARRAY<ARRAY<STRING>>>',
      map('primitivesAsString', 'true')
    ) AS parsed
  FROM bronze.analytics_video_country_daily_raw AS r
),
exploded_country_rows AS (
  SELECT
    p.snapshot_date,
    p.ingest_ts_utc,
    p.request_id,
    p.run_id,
    p.schema_version,
    transform(p.parsed.columnHeaders, x -> x.name) AS header_names,
    explode_outer(p.parsed.rows) AS row_values
  FROM parsed_country_payload AS p
),
typed_country_rows AS (
  SELECT
    CASE
      WHEN array_position(header_names, 'video') > 0
        THEN element_at(row_values, CAST(array_position(header_names, 'video') AS INT))
      ELSE NULL
    END AS video_id,
    CASE
      WHEN array_position(header_names, 'day') > 0
        THEN COALESCE(
          to_date(element_at(row_values, CAST(array_position(header_names, 'day') AS INT))),
          snapshot_date
        )
      ELSE snapshot_date
    END AS date,
    upper(element_at(row_values, CAST(array_position(header_names, 'country') AS INT))) AS country_code,
    CAST(
      CASE
        WHEN array_position(header_names, 'views') > 0
          THEN element_at(row_values, CAST(array_position(header_names, 'views') AS INT))
        ELSE NULL
      END AS BIGINT
    ) AS views,
    CAST(
      CASE
        WHEN array_position(header_names, 'estimatedMinutesWatched') > 0
          THEN element_at(row_values, CAST(array_position(header_names, 'estimatedMinutesWatched') AS INT))
        ELSE NULL
      END AS BIGINT
    ) AS estimated_minutes_watched,
    snapshot_date,
    ingest_ts_utc,
    request_id,
    run_id,
    schema_version
  FROM exploded_country_rows
),
dedup_country_metrics AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY video_id, date, country_code
      ORDER BY snapshot_date DESC, ingest_ts_utc DESC, request_id DESC
    ) AS rn
  FROM typed_country_rows
  WHERE video_id IS NOT NULL
    AND date IS NOT NULL
    AND country_code IS NOT NULL
    AND country_code <> ''
)
SELECT
  video_id,
  date,
  country_code,
  views,
  estimated_minutes_watched,
  snapshot_date,
  ingest_ts_utc,
  request_id,
  run_id,
  schema_version
FROM dedup_country_metrics
WHERE rn = 1;

CREATE OR REFRESH MATERIALIZED VIEW silver.fact_video_device_metrics
AS
WITH parsed_device_payload AS (
  SELECT
    r.snapshot_date,
    r.ingest_ts_utc,
    r.request_id,
    r.run_id,
    r.schema_version,
    from_json(
      r.payload,
      'STRUCT<columnHeaders: ARRAY<STRUCT<name: STRING, columnType: STRING, dataType: STRING>>, rows: ARRAY<ARRAY<STRING>>>',
      map('primitivesAsString', 'true')
    ) AS parsed
  FROM bronze.analytics_video_device_daily_raw AS r
),
exploded_device_rows AS (
  SELECT
    p.snapshot_date,
    p.ingest_ts_utc,
    p.request_id,
    p.run_id,
    p.schema_version,
    transform(p.parsed.columnHeaders, x -> x.name) AS header_names,
    explode_outer(p.parsed.rows) AS row_values
  FROM parsed_device_payload AS p
),
typed_device_rows AS (
  SELECT
    CASE
      WHEN array_position(header_names, 'video') > 0
        THEN element_at(row_values, CAST(array_position(header_names, 'video') AS INT))
      ELSE NULL
    END AS video_id,
    CASE
      WHEN array_position(header_names, 'day') > 0
        THEN COALESCE(
          to_date(element_at(row_values, CAST(array_position(header_names, 'day') AS INT))),
          snapshot_date
        )
      ELSE snapshot_date
    END AS date,
    upper(element_at(row_values, CAST(array_position(header_names, 'deviceType') AS INT))) AS device_type,
    CAST(
      CASE
        WHEN array_position(header_names, 'views') > 0
          THEN element_at(row_values, CAST(array_position(header_names, 'views') AS INT))
        ELSE NULL
      END AS BIGINT
    ) AS views,
    CAST(
      CASE
        WHEN array_position(header_names, 'estimatedMinutesWatched') > 0
          THEN element_at(row_values, CAST(array_position(header_names, 'estimatedMinutesWatched') AS INT))
        ELSE NULL
      END AS BIGINT
    ) AS estimated_minutes_watched,
    snapshot_date,
    ingest_ts_utc,
    request_id,
    run_id,
    schema_version
  FROM exploded_device_rows
),
dedup_device_metrics AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY video_id, date, device_type
      ORDER BY snapshot_date DESC, ingest_ts_utc DESC, request_id DESC
    ) AS rn
  FROM typed_device_rows
  WHERE video_id IS NOT NULL
    AND date IS NOT NULL
    AND device_type IS NOT NULL
    AND device_type <> ''
)
SELECT
  video_id,
  date,
  device_type,
  views,
  estimated_minutes_watched,
  snapshot_date,
  ingest_ts_utc,
  request_id,
  run_id,
  schema_version
FROM dedup_device_metrics
WHERE rn = 1;

CREATE OR REFRESH MATERIALIZED VIEW silver.dim_traffic_source
AS
WITH parsed_traffic_payload AS (
  SELECT
    r.snapshot_date,
    r.ingest_ts_utc,
    r.request_id,
    r.run_id,
    r.schema_version,
    from_json(
      r.payload,
      'STRUCT<columnHeaders: ARRAY<STRUCT<name: STRING, columnType: STRING, dataType: STRING>>, rows: ARRAY<ARRAY<STRING>>>',
      map('primitivesAsString', 'true')
    ) AS parsed
  FROM bronze.analytics_video_traffic_source_daily_raw AS r
),
typed_traffic_rows AS (
  SELECT
    upper(element_at(row_values, CAST(array_position(header_names, 'insightTrafficSourceType') AS INT))) AS source_id,
    snapshot_date,
    ingest_ts_utc,
    request_id,
    run_id,
    schema_version
  FROM (
    SELECT
      snapshot_date,
      ingest_ts_utc,
      request_id,
      run_id,
      schema_version,
      transform(parsed.columnHeaders, x -> x.name) AS header_names,
      explode_outer(parsed.rows) AS row_values
    FROM parsed_traffic_payload
  ) AS e
),
dedup_traffic_sources AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY source_id
      ORDER BY snapshot_date DESC, ingest_ts_utc DESC, request_id DESC
    ) AS rn
  FROM typed_traffic_rows
  WHERE source_id IS NOT NULL AND source_id <> ''
)
SELECT
  source_id,
  source_id AS source_name,
  snapshot_date,
  ingest_ts_utc,
  request_id,
  run_id,
  schema_version
FROM dedup_traffic_sources
WHERE rn = 1;

CREATE OR REFRESH MATERIALIZED VIEW silver.dim_country
AS
WITH parsed_country_payload AS (
  SELECT
    r.snapshot_date,
    r.ingest_ts_utc,
    r.request_id,
    r.run_id,
    r.schema_version,
    from_json(
      r.payload,
      'STRUCT<columnHeaders: ARRAY<STRUCT<name: STRING, columnType: STRING, dataType: STRING>>, rows: ARRAY<ARRAY<STRING>>>',
      map('primitivesAsString', 'true')
    ) AS parsed
  FROM bronze.analytics_video_country_daily_raw AS r
),
typed_country_rows AS (
  SELECT
    upper(element_at(row_values, CAST(array_position(header_names, 'country') AS INT))) AS country_code,
    snapshot_date,
    ingest_ts_utc,
    request_id,
    run_id,
    schema_version
  FROM (
    SELECT
      snapshot_date,
      ingest_ts_utc,
      request_id,
      run_id,
      schema_version,
      transform(parsed.columnHeaders, x -> x.name) AS header_names,
      explode_outer(parsed.rows) AS row_values
    FROM parsed_country_payload
  ) AS e
),
dedup_countries AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY country_code
      ORDER BY snapshot_date DESC, ingest_ts_utc DESC, request_id DESC
    ) AS rn
  FROM typed_country_rows
  WHERE country_code IS NOT NULL AND country_code <> ''
)
SELECT
  d.country_code,
  COALESCE(ref.country_name, d.country_code) AS country_name,
  d.snapshot_date,
  d.ingest_ts_utc,
  d.request_id,
  d.run_id,
  d.schema_version
FROM dedup_countries AS d
LEFT JOIN silver.dim_country_reference AS ref
  ON d.country_code = ref.country_code
WHERE d.rn = 1;

CREATE OR REFRESH MATERIALIZED VIEW silver.dim_device
AS
WITH parsed_device_payload AS (
  SELECT
    r.snapshot_date,
    r.ingest_ts_utc,
    r.request_id,
    r.run_id,
    r.schema_version,
    from_json(
      r.payload,
      'STRUCT<columnHeaders: ARRAY<STRUCT<name: STRING, columnType: STRING, dataType: STRING>>, rows: ARRAY<ARRAY<STRING>>>',
      map('primitivesAsString', 'true')
    ) AS parsed
  FROM bronze.analytics_video_device_daily_raw AS r
),
typed_device_rows AS (
  SELECT
    upper(element_at(row_values, CAST(array_position(header_names, 'deviceType') AS INT))) AS device_type,
    snapshot_date,
    ingest_ts_utc,
    request_id,
    run_id,
    schema_version
  FROM (
    SELECT
      snapshot_date,
      ingest_ts_utc,
      request_id,
      run_id,
      schema_version,
      transform(parsed.columnHeaders, x -> x.name) AS header_names,
      explode_outer(parsed.rows) AS row_values
    FROM parsed_device_payload
  ) AS e
),
dedup_devices AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY device_type
      ORDER BY snapshot_date DESC, ingest_ts_utc DESC, request_id DESC
    ) AS rn
  FROM typed_device_rows
  WHERE device_type IS NOT NULL AND device_type <> ''
)
SELECT
  device_type,
  device_type AS device_name,
  snapshot_date,
  ingest_ts_utc,
  request_id,
  run_id,
  schema_version
FROM dedup_devices
WHERE rn = 1;

CREATE OR REFRESH MATERIALIZED VIEW silver.dim_date
AS
WITH all_metric_dates AS (
  SELECT date FROM silver.fact_channel_daily_metrics WHERE date IS NOT NULL
  UNION
  SELECT date FROM silver.fact_video_daily_metrics WHERE date IS NOT NULL
  UNION
  SELECT date FROM silver.fact_video_traffic_source_metrics WHERE date IS NOT NULL
  UNION
  SELECT date FROM silver.fact_video_country_metrics WHERE date IS NOT NULL
  UNION
  SELECT date FROM silver.fact_video_device_metrics WHERE date IS NOT NULL
)
SELECT
  date,
  year(date) AS year,
  month(date) AS month,
  day(date) AS day_of_month,
  dayofweek(date) AS day_of_week,
  CASE
    WHEN dayofweek(date) IN (1, 7) THEN TRUE
    ELSE FALSE
  END AS is_weekend
FROM all_metric_dates;


