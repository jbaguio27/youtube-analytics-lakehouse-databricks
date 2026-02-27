{{ config(severity='warn') }}

with observed as (
  select distinct upper(source_id) as source_id
  from {{ ref('gold_video_traffic_source_daily_summary') }}
  where source_id is not null and trim(source_id) <> ''
),
known as (
  select stack(
    22,
    'ADVERTISING',
    'ANNOTATION',
    'CAMPAIGN_CARD',
    'END_SCREEN',
    'EXT_URL',
    'HASHTAGS',
    'LIVE_REDIRECT',
    'NO_LINK_EMBEDDED',
    'NO_LINK_OTHER',
    'NOTIFICATION',
    'PLAYLIST',
    'PRODUCT_PAGE',
    'PROMOTED',
    'SHORTS',
    'SOUND_PAGE',
    'SUBSCRIBER',
    'VIDEO_REMIXES',
    'YT_CHANNEL',
    'YT_OTHER_PAGE',
    'YT_SEARCH',
    'YT_WATCH_PAGE',
    'UNKNOWN'
  ) as source_id
)
select o.source_id
from observed o
left join known k
  on o.source_id = k.source_id
where k.source_id is null
