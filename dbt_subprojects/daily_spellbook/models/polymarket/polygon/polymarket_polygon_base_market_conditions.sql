{{
  config(
    schema = 'polymarket_polygon',
    alias = 'base_market_conditions',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['condition_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

with conditions as (
  select
    evt_block_time as block_time,
    evt_block_number as block_number,
    conditionId as condition_id,
    questionId as question_id,
    outcomeSlotCount as outcome_slot_count,
    oracle,
    evt_index,
    evt_tx_hash as tx_hash
  from {{ source('polymarket_polygon', 'ctf_evt_ConditionPreparation') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
)

select
  c.block_time,
  c.block_number,
  c.condition_id,
  c.question_id,
  c.outcome_slot_count,
  c.oracle,
  c.evt_index,
  c.tx_hash
from conditions c
  inner join {{ ref('polymarket_polygon_markets') }} m on c.question_id = m.question_id
