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

with metadata_recompute_tokens as (
  {% if is_incremental() %}
  select
    rt.token_id,
    rt.recompute_from_day
  from {{ ref('polymarket_polygon_position_metadata_recompute_tokens') }} as rt
  where {{ incremental_predicate('rt.change_detected_at') }}
  {% else %}
  select
    cast(null as uint256) as token_id,
    cast(null as date) as recompute_from_day
  where 1 = 0
  {% endif %}
),

positions_base as (
  select
    p.day,
    p.address,
    p.token_id,
    p.balance,
    p.price,
    p.usd_value,
    p._updated_at
  from {{ ref('polymarket_polygon_positions_enriched_base') }} as p
  where 1=1
    and p.day >= date '2025-01-01' -- ci test only
  {% if is_incremental() %}
    and {{ incremental_predicate('p._updated_at') }}
  union all
  select
    p.day,
    p.address,
    p.token_id,
    p.balance,
    p.price,
    p.usd_value,
    p._updated_at
  from {{ ref('polymarket_polygon_positions_enriched_base') }} as p
  inner join metadata_recompute_tokens as mrt
    on p.token_id = mrt.token_id
    and p.day >= mrt.recompute_from_day
  where not ({{ incremental_predicate('p._updated_at') }})
  {% endif %}
),

market_details as (
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
    market_end_time,
    outcome as market_outcome,
    resolved_on_timestamp,
    last_uploaded_at
  from {{ ref('polymarket_polygon_market_details') }}
),

positions as (
  select
    pb.day,
    pb.address,
    md.unique_key,
    pb.token_id,
    md.token_outcome,
    md.token_outcome_name,
    pb.balance,
    pb.price,
    pb.usd_value,
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
    md.resolved_on_timestamp,
    greatest(
      coalesce(pb._updated_at, timestamp '1970-01-01 00:00:00'),
      coalesce(md.last_uploaded_at, timestamp '1970-01-01 00:00:00'),
      coalesce(md.resolved_on_timestamp, timestamp '1970-01-01 00:00:00')
    ) as _updated_at
  from positions_base as pb
  inner join market_details as md
    on pb.token_id = md.token_id
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
  _updated_at
from positions
