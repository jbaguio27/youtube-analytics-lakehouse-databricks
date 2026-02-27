with all_metrics as (
  select 'gold_channel_daily_summary' as model_name, views, comments, estimated_minutes_watched
  from {{ ref('gold_channel_daily_summary') }}
  union all
  select 'gold_video_daily_summary' as model_name, views, comments, estimated_minutes_watched
  from {{ ref('gold_video_daily_summary') }}
  union all
  select 'gold_video_country_daily_summary' as model_name, views, cast(null as bigint) as comments, estimated_minutes_watched
  from {{ ref('gold_video_country_daily_summary') }}
  union all
  select 'gold_video_device_daily_summary' as model_name, views, cast(null as bigint) as comments, estimated_minutes_watched
  from {{ ref('gold_video_device_daily_summary') }}
  union all
  select 'gold_video_traffic_source_daily_summary' as model_name, views, cast(null as bigint) as comments, estimated_minutes_watched
  from {{ ref('gold_video_traffic_source_daily_summary') }}
)
select *
from all_metrics
where coalesce(views, 0) < 0
   or coalesce(comments, 0) < 0
   or coalesce(estimated_minutes_watched, 0) < 0
