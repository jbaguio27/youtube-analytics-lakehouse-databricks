select
  f.video_id,
  v.channel_id,
  f.date,
  f.views,
  f.likes,
  f.comments,
  f.estimated_minutes_watched,
  f.average_view_duration_seconds,
  f.snapshot_date,
  f.ingest_ts_utc
from {{ source('silver', 'fact_video_daily_metrics') }} as f
left join {{ source('silver', 'silver_videos') }} as v
  on f.video_id = v.video_id
