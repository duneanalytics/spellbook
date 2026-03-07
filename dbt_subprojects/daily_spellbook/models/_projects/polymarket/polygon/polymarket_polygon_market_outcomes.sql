{{
  config(
    schema = 'polymarket_polygon',
    alias = 'market_outcomes',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['question_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , post_hook='{{ hide_spells() }}'
  )
}}

with all_outcomes as (
  select
    evt_block_time as block_time,
    evt_block_number as block_number,
    'uma-v1' as source,
    questionID as question_id,
    case settledPrice / 1e18
      when 1 then 'yes'
      when 0.5 then '50/50'
      else 'no'
    end as outcome,
    evt_index,
    evt_tx_hash as tx_hash
  from {{ source('minereum_polygon', 'UmaConditionalTokensBinaryAdapter_evt_QuestionSettled') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}

  union all

  select
    evt_block_time as block_time,
    evt_block_number as block_number,
    'uma-v2' as source,
    questionID as question_id,
    case settledPrice / 1e18
      when 1 then 'yes'
      when 0.5 then '50/50'
      else 'no'
    end as outcome,
    evt_index,
    evt_tx_hash as tx_hash
  from {{ source('polymarket_polygon', 'UmaCtfAdapter_evt_QuestionResolved') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}

  union all

  select
    evt_block_time as block_time,
    evt_block_number as block_number,
    'polymarket' as source,
    questionId as question_id,
    if(outcome, 'yes', 'no') as outcome,
    evt_index,
    evt_tx_hash as tx_hash
  from {{ source('polymarket_polygon', 'NegRiskAdapter_evt_OutcomeReported') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}

  union all

  -- CTF-level ConditionResolution catches resolutions from ALL oracle adapters,
  -- including newer contracts (v3+) not tracked by the adapter-specific tables above
  select
    cr.evt_block_time as block_time,
    cr.evt_block_number as block_number,
    'ctf' as source,
    cr.questionId as question_id,
    case
      when cr.payoutNumerators[1] = 1 and cr.payoutNumerators[2] = 0 then 'yes'
      when cr.payoutNumerators[1] = 0 and cr.payoutNumerators[2] = 1 then 'no'
      when cr.payoutNumerators[1] = 1 and cr.payoutNumerators[2] = 1 then '50/50'
      else 'unknown'
    end as outcome,
    cr.evt_index,
    cr.evt_tx_hash as tx_hash
  from {{ source('polymarket_polygon', 'ctf_evt_ConditionResolution') }} cr
  {% if is_incremental() %}
  where {{ incremental_predicate('cr.evt_block_time') }}
  {% endif %}
)

-- Deduplicate: prefer adapter-specific sources over CTF for consistent outcome derivation
select
  block_time,
  block_number,
  source,
  question_id,
  outcome,
  evt_index,
  tx_hash
from (
  select
    *,
    row_number() over (
      partition by question_id
      order by case source when 'ctf' then 1 else 0 end, block_time
    ) as rn
  from all_outcomes
)
where rn = 1
