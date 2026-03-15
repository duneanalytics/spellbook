-- depends_on: {{ ref('polymarket_polygon_market_price_recompute_tokens') }}

{{ config(
    schema = 'polymarket_polygon',
    alias = 'market_prices_hourly',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['block_month'],
    unique_key = ['block_month', 'hour', 'token_id'],
    post_hook = '{{ expose_spells(blockchains = \'["polygon"]\',
                                  spell_type = "project",
                                  spell_name = "polymarket",
                                  contributors = \'["0xboxer, tomfutago"]\') }}'
  )
}}

-- no incremental_predicates: resolution-driven recomputes can target old hours, and
-- filtering DBT_INTERNAL_DEST by recent time would miss matches and create duplicates.

-- derive the global run boundaries (recent trade window and current end hour)
with trade_bounds as (
  select
    {% if is_incremental() -%}
    coalesce(
      (select date_add('day', -2, max(hour)) from {{ this }}),
      (select cast(date_trunc('hour', min(block_time)) as timestamp) from {{ ref('polymarket_polygon_market_trades_raw') }})
    ) as trade_start_hour,
    {% else -%}
    (select cast(date_trunc('hour', min(block_time)) as timestamp) from {{ ref('polymarket_polygon_market_trades_raw') }}) as trade_start_hour,
    {% endif -%}
    cast(date_trunc('hour', current_timestamp) as timestamp) as end_hour
),

-- build token-specific windows so one old recompute token does not force a global backfill
{% if is_incremental() -%}
recent_tokens_from_this as (
  select
    t.token_id,
    min(tb.trade_start_hour) as start_hour
  from {{ this }} t
  cross join trade_bounds tb
  where t.hour = (select max(hour) from {{ this }})
    and t.token_id is not null
  group by 1
),

recent_trade_tokens as (
  select
    t.asset_id as token_id,
    min(tb.trade_start_hour) as start_hour
  from {{ ref('polymarket_polygon_market_trades_raw') }} t
  cross join trade_bounds tb
  where t.block_time >= tb.trade_start_hour
    and t.block_time < tb.end_hour
  group by 1
),

resolution_tokens as (
  select
    rt.token_id,
    rt.recompute_from_hour as start_hour
  from {{ ref('polymarket_polygon_market_price_recompute_tokens') }} rt
  cross join trade_bounds tb
  where rt.change_detected_at >= tb.trade_start_hour
),

token_windows as (
  select
    token_id,
    min(start_hour) as start_hour
  from (
    select token_id, start_hour from recent_tokens_from_this
    union all
    select token_id, start_hour from recent_trade_tokens
    union all
    select token_id, start_hour from resolution_tokens
  ) token_inputs
  where token_id is not null
  group by 1
),
{% else -%}
token_windows as (
  select
    t.asset_id as token_id,
    cast(date_trunc('hour', min(t.block_time)) as timestamp) as start_hour
  from {{ ref('polymarket_polygon_market_trades_raw') }} t
  where t.asset_id is not null
  group by 1
),
{% endif -%}

params as (
  select
    tb.end_hour
  from trade_bounds tb
),

-- keep raw trade updates for each token only inside that token's window
window_updates as (
  select
    t.block_time,
    t.condition_id,
    t.asset_id as token_id,
    t.price,
    t.evt_index,
    t.tx_hash
  from {{ ref('polymarket_polygon_market_trades_raw') }} t
  inner join token_windows tw
    on tw.token_id = t.asset_id
  cross join params p
  where t.block_time >= tw.start_hour
    and t.block_time < p.end_hour
),

-- carry forward the last pre-window hourly value per token to anchor forward-fill
pre_window_latest as (
  {% if is_incremental() -%}
  select
    t.hour as block_time,
    t.condition_id,
    t.token_id,
    t.price,
    cast(null as bigint) as evt_index,
    cast(null as varbinary) as tx_hash
  from {{ this }} t
  inner join token_windows tw
    on tw.token_id = t.token_id
    and t.hour = date_add('hour', -1, tw.start_hour)
  {% else -%}
  select
    cast(null as timestamp) as block_time,
    cast(null as varbinary) as condition_id,
    cast(null as uint256) as token_id,
    cast(null as double) as price,
    cast(null as bigint) as evt_index,
    cast(null as varbinary) as tx_hash
  where 1 = 0
  {% endif -%}
),

-- merge in-window updates with pre-window anchors into one update stream
all_updates as (
  select
    block_time,
    condition_id,
    token_id,
    price,
    evt_index,
    tx_hash
  from window_updates
  union all
  select
    block_time,
    condition_id,
    token_id,
    price,
    evt_index,
    tx_hash
  from pre_window_latest
),

-- keep the last update per token/hour and compute the next update boundary for interval filling
changed_prices as (
  select
    date_trunc('hour', block_time) as hour,
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
      evt_index,
      tx_hash,
      -- deterministic tie-break for rows sharing the same block_time within one token-hour
      row_number() over (
        partition by date_trunc('hour', block_time), token_id
        order by block_time desc, evt_index asc nulls last, tx_hash asc nulls last
      ) as rn
    from all_updates
  ) ranked
  where rn = 1
),

-- enumerate each token-hour in scope from token start through run end
token_hours as (
  select
    tw.token_id,
    h.timestamp as hour
  from token_windows tw
  inner join {{ source('utils', 'hours') }} h
    on h.timestamp >= tw.start_hour
  cross join params p
  where h.timestamp < p.end_hour
),

-- forward-fill each token price across its own hour window until next observed update
forward_fill as (
  select
    cast(th.hour as timestamp) as hour,
    lp.condition_id,
    th.token_id,
    lp.price
  from token_hours th
  inner join changed_prices lp
    on th.token_id = lp.token_id
    and th.hour >= lp.hour
    and (lp.next_update_hour is null or th.hour < lp.next_update_hour)
),

-- parse market end timestamps and resolution metadata used for post-resolution price correction
market_details_enriched as (
  select
    md.token_id,
    try_cast(substring(md.market_end_time from 1 for 19) as timestamp) as market_end_time_ts,
    md.token_outcome,
    md.outcome
  from {{ ref('polymarket_polygon_market_details') }} md
),

-- enforce resolved market terminal prices after market end while preserving live pricing before end
price_correction as (
  select
    cast(date_trunc('month', ff.hour) as date) as block_month,
    ff.hour,
    ff.condition_id,
    ff.token_id,
    case
      when ff.hour <= md.market_end_time_ts then ff.price
      when ff.hour > md.market_end_time_ts then
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
  left join market_details_enriched md on ff.token_id = md.token_id
)

select
  pc.block_month,
  pc.hour,
  pc.condition_id,
  pc.token_id,
  pc.price
from price_correction pc
where pc.price is not null
