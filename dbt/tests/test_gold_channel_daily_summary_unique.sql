select
  channel_id,
  date,
  count(*) as row_count
from {{ ref('gold_channel_daily_summary') }}
group by channel_id, date
having count(*) > 1
