select
  video_id,
  date,
  source_id,
  count(*) as row_count
from {{ ref('gold_video_traffic_source_daily_summary') }}
group by video_id, date, source_id
having count(*) > 1
