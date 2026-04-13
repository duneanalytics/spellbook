{{
  config(
    schema = 'polymarket_polygon',
    alias = 'positions_enriched_base',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['day'],
    unique_key = ['day', 'address', 'token_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')],
    merge_skip_unchanged = true
  )
}}

with target_positions as (
  select
    p.day,
    p.address,
    p.token_id,
    p.balance,
    p.last_updated
  from {{ ref('polymarket_polygon_positions_raw') }} as p
  {% if is_incremental() %}
  where {{ incremental_predicate('p.day') }}
  {% else %}
  where p.day >= date '2020-09-03'
  {% endif %}
),

priced_positions as (
  select
    tp.day,
    tp.address,
    tp.token_id,
    tp.balance,
    coalesce(dp.price, 0) as price,
    tp.balance * coalesce(dp.price, 0) as usd_value,
    greatest(
      coalesce(tp.last_updated, timestamp '1970-01-01 00:00:00'),
      coalesce(dp._updated_at, timestamp '1970-01-01 00:00:00')
    ) as _updated_at
  from target_positions as tp
  left join {{ ref('polymarket_polygon_market_prices_daily') }} as dp
    on tp.day = dp.day
    and tp.token_id = dp.token_id
)

select
  day,
  address,
  token_id,
  balance,
  price,
  usd_value,
  _updated_at
from priced_positions
