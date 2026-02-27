select
  video_id,
  date,
  device_type,
  count(*) as row_count
from {{ ref('gold_video_device_daily_summary') }}
group by video_id, date, device_type
having count(*) > 1
