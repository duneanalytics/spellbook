{{
  config(
    schema = 'polymarket_polygon',
    alias = 'positions',
    materialized = 'view',
    post_hook = '{{ expose_spells(blockchains = \'["polygon"]\',
                                  spell_type = "project",
                                  spell_name = "polymarket",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

with positions_base as (
  select
    p.day,
    p.address,
    p.token_id,
    p.balance,
    p.price,
    p.usd_value,
    p._updated_at
  from {{ ref('polymarket_polygon_positions_enriched_base') }} as p
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
