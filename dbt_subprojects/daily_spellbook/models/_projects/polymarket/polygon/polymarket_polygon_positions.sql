{{
  config(
    schema = 'polymarket_polygon',
    alias = 'positions',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['block_month'],
    unique_key = ['day', 'address', 'token_id'],
    merge_skip_unchanged = true,
    post_hook = '{{ private_data_explorer(blockchains = \'["polygon"]\',
                                  spell_type = "project",
                                  spell_name = "polymarket") }}'
  )
}}

{% if is_incremental() -%}

-- Triggers only on outcome / resolved_on_timestamp changes; other market flags are snapshot-at-write.
-- Full scan of {{ this }} preserves drift recovery for any token with history, matching prod.
with changed_markets as (
  select distinct
    mm_check.token_id,
    mm_check.market_start_time,
    mm_check.resolved_on_timestamp
  from {{ ref('polymarket_polygon_market_details') }} as mm_check
  inner join {{ this }} as existing
    on mm_check.token_id = existing.token_id
    and existing.day >= cast(mm_check.market_start_time as date)
  where existing.market_outcome is distinct from mm_check.outcome
    or existing.resolved_on_timestamp is distinct from mm_check.resolved_on_timestamp
),

-- Scalar bound pushed as a partition predicate; per-row day-range can't be.
changed_markets_day_bounds as (
  select min(cast(market_start_time as date)) as min_day
  from changed_markets
),

positions_to_emit as (
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
    and p.day between cast(cm.market_start_time as date)
                  and cast(cm.resolved_on_timestamp as date)
  where not ({{ incremental_predicate('p.day') }})
    and p.day >= (select min_day from changed_markets_day_bounds)
)

select
  date_trunc('month', p.day) as block_month,
  p.day,
  p.address,
  mm.unique_key,
  p.token_id,
  mm.token_outcome,
  mm.token_outcome_name,
  p.balance,
  mm.question_id,
  mm.question as market_question,
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
  mm.resolved_on_timestamp,
  now() as _updated_at
from positions_to_emit as p
inner join {{ ref('polymarket_polygon_market_details') }} as mm
  on p.token_id = mm.token_id

{%- else -%}

select
  date_trunc('month', p.day) as block_month,
  p.day,
  p.address,
  mm.unique_key,
  p.token_id,
  mm.token_outcome,
  mm.token_outcome_name,
  p.balance,
  mm.question_id,
  mm.question as market_question,
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
  mm.resolved_on_timestamp,
  now() as _updated_at
from {{ ref('polymarket_polygon_positions_raw') }} as p
inner join {{ ref('polymarket_polygon_market_details') }} as mm
  on p.token_id = mm.token_id

{%- endif %}
