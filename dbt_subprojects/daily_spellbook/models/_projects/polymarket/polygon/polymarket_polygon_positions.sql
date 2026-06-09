{{
  config(
    schema = 'polymarket_polygon',
    alias = 'positions',
    materialized = 'view',
    post_hook = '{{ private_data_explorer(blockchains = \'["polygon"]\',
                                  spell_type = "project",
                                  spell_name = "polymarket") }}'
  )
}}

select
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
  p.last_updated as _updated_at
from {{ ref('polymarket_polygon_positions_raw') }} as p
inner join {{ ref('polymarket_polygon_market_details') }} as mm
  on p.token_id = mm.token_id
