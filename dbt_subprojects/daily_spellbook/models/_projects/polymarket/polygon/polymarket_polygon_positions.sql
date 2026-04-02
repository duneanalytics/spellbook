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

with positions as (
  select
    p.day,
    p.address,
    mm.unique_key,
    p.token_id,
    mm.token_outcome,
    mm.token_outcome_name,
    p.balance,
    mm.question_id,
    mm.question AS market_question,
    mm.market_description,
    mm.event_market_name,
    mm.event_market_description,
    mm.active,
    mm.closed,
    mm.accepting_orders,
    mm.polymarket_link,
    mm.market_start_time,
    mm.market_end_time,
    mm.outcome as market_outcome,
    mm.resolved_on_timestamp
  from {{ ref('polymarket_polygon_positions_raw') }} as p
  inner join {{ ref('polymarket_polygon_market_details') }} as mm on p.token_id = mm.token_id
)

{% if is_incremental() -%}

, changed_markets AS (
  select distinct
    mm_check.token_id
  from {{ ref('polymarket_polygon_market_details') }} as mm_check
  inner join {{ this }} as existing
    on mm_check.token_id = existing.token_id
    and existing.day >= cast(try_cast(mm_check.market_start_time as timestamp) as date)
  where existing.active is distinct from mm_check.active
    or existing.closed is distinct from mm_check.closed
    or existing.accepting_orders is distinct from mm_check.accepting_orders
    or existing.market_outcome is distinct from mm_check.outcome
    or existing.resolved_on_timestamp is distinct from mm_check.resolved_on_timestamp
),

recent_positions AS (
  select
    jp.day,
    jp.address,
    jp.unique_key,
    jp.token_id,
    jp.token_outcome,
    jp.token_outcome_name,
    jp.balance,
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
  cast(day as timestamp) as _updated_at
from positions
where day >= date '2026-03-26' -- temporary CI filter, revert before merge

{% endif -%}
