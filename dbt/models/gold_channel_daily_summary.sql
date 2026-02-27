select
  channel_id,
  date,
  views,
  likes,
  comments,
  estimated_minutes_watched,
  subscribers_gained,
  subscribers_lost,
  (subscribers_gained - subscribers_lost) as net_subscribers,
  snapshot_date,
  ingest_ts_utc
from {{ source('silver', 'fact_channel_daily_metrics') }}
