select
  f.video_id,
  v.channel_id,
  f.date,
  f.device_type,
  d.device_name,
  f.views,
  coalesce(f.estimated_minutes_watched, 0) as estimated_minutes_watched,
  f.snapshot_date,
  f.ingest_ts_utc
from {{ source('silver', 'fact_video_device_metrics') }} as f
left join {{ source('silver', 'silver_videos') }} as v
  on f.video_id = v.video_id
left join {{ source('silver', 'dim_device') }} as d
  on f.device_type = d.device_type
