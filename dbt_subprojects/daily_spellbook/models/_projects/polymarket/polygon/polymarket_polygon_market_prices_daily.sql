{{
  config(
    schema = 'polymarket_polygon',
    alias = 'market_prices_daily',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['day'],
    unique_key = ['day', 'token_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')],
    merge_skip_unchanged = true
  )
}}

with hourly_source as (
  select
    cast(h.hour as date) as day,
    h.condition_id,
    h.token_id,
    h.hour,
    h.price
  from {{ ref('polymarket_polygon_market_prices_hourly') }} as h
  {% if is_incremental() %}
  where {{ incremental_predicate('h.hour') }}
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
