-- depends_on: {{ ref('polymarket_polygon_market_state_recompute_tokens') }}

{{
  config(
    schema = 'polymarket_polygon',
    alias = 'positions',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['day'],
    unique_key = ['day', 'address', 'token_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')],
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
    market_end_time,
    outcome as market_outcome,
    resolved_on_timestamp
  from {{ ref('polymarket_polygon_market_details') }}
),

{% if is_incremental() %}
changed_markets as (
  select
    token_id,
    market_start_day
  from {{ ref('polymarket_polygon_market_state_recompute_tokens') }}
  where {{ incremental_predicate('change_detected_at') }}
),

target_positions as (
  select
    p.day,
    p.address,
    p.token_id,
    p.balance
  from {{ ref('polymarket_polygon_positions_raw') }} as p
  where {{ incremental_predicate('p.day') }}

  union all

  select
    p.day,
    p.address,
    p.token_id,
    p.balance
  from {{ ref('polymarket_polygon_positions_raw') }} as p
  inner join changed_markets as cm
    on p.token_id = cm.token_id
    and p.day >= cm.market_start_day
  where not ({{ incremental_predicate('p.day') }})
),
{% else %}
target_positions as (
  select
    p.day,
    p.address,
    p.token_id,
    p.balance
  from {{ ref('polymarket_polygon_positions_raw') }} as p
  where p.day >= date '2020-09-03'
),
{% endif %}

priced_positions as (
  select
    tp.day,
    tp.address,
    tp.token_id,
    tp.balance,
    coalesce(dp.price, 0) as price,
    tp.balance * coalesce(dp.price, 0) as usd_value
  from target_positions as tp
  left join {{ ref('polymarket_polygon_market_prices_daily') }} as dp
    on tp.day = dp.day
    and tp.token_id = dp.token_id
),

positions as (
  select
    pp.day,
    pp.address,
    md.unique_key,
    pp.token_id,
    md.token_outcome,
    md.token_outcome_name,
    pp.balance,
    pp.price,
    pp.usd_value,
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
  from priced_positions as pp
  inner join market_details as md on pp.token_id = md.token_id
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
from positions
