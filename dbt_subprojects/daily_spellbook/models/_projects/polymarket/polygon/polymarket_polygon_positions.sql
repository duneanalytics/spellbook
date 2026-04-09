{{
  config(
    schema = 'polymarket_polygon',
    alias = 'positions',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['day'],
    unique_key = ['day', 'address', 'token_id'],
    merge_skip_unchanged = true,
    post_hook = '{{ expose_spells(blockchains = \'["polygon"]\',
                                  spell_type = "project",
                                  spell_name = "polymarket",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

with market_details as (
  select
    token_id,
    unique_key,
    token_outcome,
    token_outcome_name,
    question_id,
    question as market_question,
    market_description,
    event_market_name,
    event_market_description,
    active,
    closed,
    accepting_orders,
    polymarket_link,
    market_start_time,
    market_start_time_parsed,
    market_end_time,
    market_end_time_parsed,
    outcome as market_outcome,
    resolved_on_timestamp,
    final_price
  from {{ ref('polymarket_polygon_market_details') }}
),

daily_prices as (
  select
    cast(pp.day as date) as day,
    pp.token_id,
    case
      when md.market_end_time_parsed is null
        or pp.day <= md.market_end_time_parsed then pp.price
      else coalesce(md.final_price, 0)
    end as price
  from {{ ref('polymarket_polygon_market_prices_daily') }} pp
  left join market_details md on pp.token_id = md.token_id
),

positions as (
  select
    p.day,
    p.address,
    md.unique_key,
    p.token_id,
    md.token_outcome,
    md.token_outcome_name,
    p.balance,
    coalesce(dp.price, 0) as price,
    p.balance * coalesce(dp.price, 0) as usd_value,
    md.question_id,
    md.market_question,
    md.market_description,
    md.event_market_name,
    md.event_market_description,
    md.active,
    md.closed,
    md.accepting_orders,
    md.polymarket_link,
    md.market_start_time,
    md.market_end_time,
    md.market_outcome,
    md.resolved_on_timestamp
  from {{ ref('polymarket_polygon_positions_raw') }} as p
  inner join market_details as md on p.token_id = md.token_id
  left join daily_prices as dp
    on p.token_id = dp.token_id
    and p.day = dp.day
)

{% if is_incremental() -%}

, changed_markets as (
  select distinct
    md.token_id
  from market_details as md
  inner join {{ this }} as existing
    on md.token_id = existing.token_id
    and existing.day >= cast(coalesce(md.market_start_time_parsed, try_cast(md.market_start_time as timestamp)) as date)
  where existing.active is distinct from md.active
    or existing.closed is distinct from md.closed
    or existing.accepting_orders is distinct from md.accepting_orders
    or existing.market_outcome is distinct from md.market_outcome
    or existing.resolved_on_timestamp is distinct from md.resolved_on_timestamp
),

recent_positions as (
  select
    jp.day,
    jp.address,
    jp.unique_key,
    jp.token_id,
    jp.token_outcome,
    jp.token_outcome_name,
    jp.balance,
    jp.price,
    jp.usd_value,
    jp.question_id,
    jp.market_question,
    jp.market_description,
    jp.event_market_name,
    jp.event_market_description,
    jp.active,
    jp.closed,
    jp.accepting_orders,
    jp.polymarket_link,
    jp.market_start_time,
    jp.market_end_time,
    jp.market_outcome,
    jp.resolved_on_timestamp
  from positions as jp
  where {{ incremental_predicate('jp.day') }}
),

historical_changed_positions as (
  select
    jp.day,
    jp.address,
    jp.unique_key,
    jp.token_id,
    jp.token_outcome,
    jp.token_outcome_name,
    jp.balance,
    jp.price,
    jp.usd_value,
    jp.question_id,
    jp.market_question,
    jp.market_description,
    jp.event_market_name,
    jp.event_market_description,
    jp.active,
    jp.closed,
    jp.accepting_orders,
    jp.polymarket_link,
    jp.market_start_time,
    jp.market_end_time,
    jp.market_outcome,
    jp.resolved_on_timestamp
  from positions as jp
  inner join changed_markets as cm on jp.token_id = cm.token_id
  where not ({{ incremental_predicate('jp.day') }})
)

select
  day,
  address,
  unique_key,
  token_id,
  token_outcome,
  token_outcome_name,
  balance,
  price,
  usd_value,
  question_id,
  market_question,
  market_description,
  event_market_name,
  event_market_description,
  active,
  closed,
  accepting_orders,
  polymarket_link,
  market_start_time,
  market_end_time,
  market_outcome,
  resolved_on_timestamp,
  now() as _updated_at
from recent_positions
union all
select
  day,
  address,
  unique_key,
  token_id,
  token_outcome,
  token_outcome_name,
  balance,
  price,
  usd_value,
  question_id,
  market_question,
  market_description,
  event_market_name,
  event_market_description,
  active,
  closed,
  accepting_orders,
  polymarket_link,
  market_start_time,
  market_end_time,
  market_outcome,
  resolved_on_timestamp,
  now() as _updated_at
from historical_changed_positions

{% else -%}

select
  day,
  address,
  unique_key,
  token_id,
  token_outcome,
  token_outcome_name,
  balance,
  price,
  usd_value,
  question_id,
  market_question,
  market_description,
  event_market_name,
  event_market_description,
  active,
  closed,
  accepting_orders,
  polymarket_link,
  market_start_time,
  market_end_time,
  market_outcome,
  resolved_on_timestamp,
  now() as _updated_at
from positions

{% endif -%}

