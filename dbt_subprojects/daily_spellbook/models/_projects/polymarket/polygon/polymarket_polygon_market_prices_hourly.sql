{{ config(
    schema = 'polymarket_polygon',
    alias = 'market_prices_hourly',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['block_month'],
    unique_key = ['block_month', 'hour', 'token_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.hour')],
    post_hook = '{{ expose_spells(blockchains = \'["polygon"]\',
                                  spell_type = "project",
                                  spell_name = "polymarket",
                                  contributors = \'["0xboxer, tomfutago"]\') }}'
  )
}}

with params as (
  select
    {% if is_incremental() -%}
    coalesce(
      (select date_add('day', -2, max(hour)) from {{ this }}),
      (select cast(date_trunc('hour', min(block_time)) as timestamp) from {{ ref('polymarket_polygon_market_trades_raw') }})
    ) as start_hour,
    {% else -%}
    (select cast(date_trunc('hour', min(block_time)) as timestamp) from {{ ref('polymarket_polygon_market_trades_raw') }}) as start_hour,
    {% endif -%}
    cast(date_trunc('hour', current_timestamp) as timestamp) as end_hour
),

window_updates as (
  select
    t.block_time,
    t.condition_id,
    t.asset_id as token_id,
    t.price
  from {{ ref('polymarket_polygon_market_trades_raw') }} t
  cross join params p
  where t.block_time >= p.start_hour
    and t.block_time < p.end_hour
),

pre_window_latest as (
  {% if is_incremental() -%}
  select
    block_time,
    condition_id,
    token_id,
    price
  from (
    select
      t.block_time,
      t.condition_id,
      t.asset_id as token_id,
      t.price,
      row_number() over (partition by t.asset_id order by t.block_time desc) as rn
    from {{ ref('polymarket_polygon_market_trades_raw') }} t
    cross join params p
    where t.block_time < p.start_hour
  ) ranked
  where rn = 1
  {% else -%}
  select
    cast(null as timestamp) as block_time,
    cast(null as varbinary) as condition_id,
    cast(null as uint256) as token_id,
    cast(null as double) as price
  where 1 = 0
  {% endif -%}
),

all_updates as (
  select
    block_time,
    condition_id,
    token_id,
    price
  from window_updates
  union all
  select
    block_time,
    condition_id,
    token_id,
    price
  from pre_window_latest
),

changed_prices as (
  select
    cast(date_trunc('hour', block_time) as timestamp) as hour,
    block_time,
    condition_id,
    token_id,
    price,
    lead(cast(date_trunc('hour', block_time) as timestamp)) over (partition by token_id order by block_time asc) as next_update_hour
  from (
    select
      block_time,
      condition_id,
      token_id,
      price,
      row_number() over (partition by date_trunc('hour', block_time), token_id order by block_time desc) as rn
    from all_updates
  ) ranked
  where rn = 1
),

hours as (
  select
    h.timestamp as hour
  from {{ source('utils', 'hours') }} h
  cross join params p
  where h.timestamp >= p.start_hour
    and h.timestamp < p.end_hour
),

forward_fill as (
  select
    cast(h.hour as timestamp) as hour,
    lp.condition_id,
    lp.token_id,
    lp.price
  from hours h
  left join changed_prices lp
    on h.hour >= lp.hour
    and (lp.next_update_hour is null or h.hour < lp.next_update_hour)
),

price_correction as (
  select
    cast(date_trunc('month', ff.hour) as date) as block_month,
    ff.hour,
    ff.condition_id,
    ff.token_id,
    case
      when ff.hour <= try_cast(substring(md.market_end_time from 1 for 19) as timestamp) then ff.price
      when ff.hour > try_cast(substring(md.market_end_time from 1 for 19) as timestamp) then
        case
          when md.token_outcome = 'Yes' and md.outcome = 'yes' then 1
          when md.token_outcome = 'Yes' and md.outcome = 'no' then 0
          when md.token_outcome = 'No' and md.outcome = 'yes' then 0
          when md.token_outcome = 'No' and md.outcome = 'no' then 1
          else ff.price
        end
      else ff.price
    end as price
  from forward_fill ff
  left join {{ ref('polymarket_polygon_market_details') }} md on ff.token_id = md.token_id
)

select
  pc.block_month,
  pc.hour,
  pc.condition_id,
  pc.token_id,
  pc.price
from price_correction pc
cross join params p
where pc.price > 0
  and pc.hour >= p.start_hour
  and pc.hour < p.end_hour
