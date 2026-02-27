select
  video_id,
  date,
  count(*) as row_count
from {{ ref('gold_video_daily_summary') }}
group by video_id, date
having count(*) > 1
