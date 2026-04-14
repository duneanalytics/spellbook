{{
  config(
    schema = 'polymarket_polygon',
    alias = 'market_prices_daily',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['block_month'],
    unique_key = ['block_month', 'day', 'token_id'],
    merge_skip_unchanged = true
  )
}}

with changed_day_tokens as (
  {% if is_incremental() %}
  select distinct
    cast(date_trunc('month', h.hour) as date) as block_month,
    cast(h.hour as date) as day,
    h.token_id
  from {{ ref('polymarket_polygon_market_prices_hourly') }} as h
  where {{ incremental_predicate('h._updated_at') }}
  {% else %}
  select
    cast(null as date) as block_month,
    cast(null as date) as day,
    cast(null as uint256) as token_id
  where 1 = 0
  {% endif %}
),

historical_day_prices as (
  select
    cast(date_trunc('month', h.hour) as date) as block_month,
    cast(h.hour as date) as day,
    h.condition_id,
    h.token_id,
    h.price,
    h._updated_at
  from {{ ref('polymarket_polygon_market_prices_hourly') }} as h
  {% if is_incremental() %}
  inner join changed_day_tokens as cdt
    on cast(date_trunc('month', h.hour) as date) = cdt.block_month
    and cast(h.hour as date) = cdt.day
    and h.token_id = cdt.token_id
  {% endif %}
  where cast(h.hour as date) < current_date
    and hour(h.hour) = 23
    and h.price > 0
),

current_day_prices as (
  select
    cast(date_trunc('month', h.hour) as date) as block_month,
    cast(h.hour as date) as day,
    max_by(h.condition_id, h.hour) as condition_id,
    h.token_id,
    max_by(h.price, h.hour) as price,
    max(h._updated_at) as _updated_at
  from {{ ref('polymarket_polygon_market_prices_hourly') }} as h
  {% if is_incremental() %}
  inner join changed_day_tokens as cdt
    on cast(date_trunc('month', h.hour) as date) = cdt.block_month
    and cast(h.hour as date) = cdt.day
    and h.token_id = cdt.token_id
  {% endif %}
  where cast(h.hour as date) = current_date
    and h.price > 0
  group by 1, 2, 4
),

daily_prices as (
  select
    block_month,
    day,
    condition_id,
    token_id,
    price,
    _updated_at
  from historical_day_prices
  union all
  select
    block_month,
    day,
    condition_id,
    token_id,
    price,
    _updated_at
  from current_day_prices
)

select
  dp.block_month,
  dp.day,
  dp.condition_id,
  dp.token_id,
  dp.price,
  dp._updated_at
from daily_prices as dp
