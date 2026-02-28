with latest_dates as (
  select 'gold_channel_daily_summary' as model_name, max(date) as max_date
  from {{ ref('gold_channel_daily_summary') }}
  union all
  select 'gold_video_daily_summary' as model_name, max(date) as max_date
  from {{ ref('gold_video_daily_summary') }}
)
select
  model_name,
  max_date,
  datediff(current_date(), max_date) as lag_days
from latest_dates
where max_date is null
   or datediff(current_date(), max_date) > {{ var('gold_freshness_max_lag_days', 7) }}
