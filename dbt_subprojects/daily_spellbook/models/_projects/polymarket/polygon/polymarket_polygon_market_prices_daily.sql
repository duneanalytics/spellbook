{{
  config(
    schema = 'polymarket_polygon',
    alias = 'market_prices_daily',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['day'],
    unique_key = ['day', 'token_id'],
    merge_skip_unchanged = true
  )
}}

with changed_day_tokens as (
  {% if is_incremental() %}
  select distinct
    cast(h.hour as date) as day,
    h.token_id
  from {{ ref('polymarket_polygon_market_prices_hourly') }} as h
  where {{ incremental_predicate('h._updated_at') }}
  {% else %}
  select
    cast(null as date) as day,
    cast(null as uint256) as token_id
  where 1 = 0
  {% endif %}
),

hourly_source as (
  select
    cast(h.hour as date) as day,
    h.condition_id,
    h.token_id,
    h.hour,
    h.price
  from {{ ref('polymarket_polygon_market_prices_hourly') }} as h
  {% if is_incremental() %}
  inner join changed_day_tokens as cdt
    on cast(h.hour as date) = cdt.day
    and h.token_id = cdt.token_id
  {% endif %}
),

daily_prices as (
  select
    hs.day,
    hs.token_id,
    max_by(hs.condition_id, hs.hour) as condition_id,
    max_by(hs.price, hs.hour) as price
  from hourly_source as hs
  group by 1, 2
)

select
  dp.day,
  dp.condition_id,
  dp.token_id,
  dp.price,
  current_timestamp as _updated_at
from daily_prices as dp
where dp.price > 0
