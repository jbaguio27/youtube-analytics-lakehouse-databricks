select
  video_id,
  date,
  country_code,
  count(*) as row_count
from {{ ref('gold_video_country_daily_summary') }}
group by video_id, date, country_code
having count(*) > 1
