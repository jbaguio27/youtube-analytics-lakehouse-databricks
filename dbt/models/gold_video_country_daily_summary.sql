select
  f.video_id,
  v.channel_id,
  f.date,
  f.country_code,
  c.country_name,
  f.views,
  coalesce(f.estimated_minutes_watched, 0) as estimated_minutes_watched,
  f.snapshot_date,
  f.ingest_ts_utc
from {{ source('silver', 'fact_video_country_metrics') }} as f
left join {{ source('silver', 'silver_videos') }} as v
  on f.video_id = v.video_id
left join {{ source('silver', 'dim_country') }} as c
  on f.country_code = c.country_code
