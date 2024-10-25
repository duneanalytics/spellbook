{{
  config(
    schema = 'polymarket_polygon',
    alias = 'market_outcomes',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['question_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    post_hook = '{{ expose_spells(blockchains = \'["polygon"]\',
                                  spell_type = "project",
                                  spell_name = "polymarket",
                                  contributors = \'["tomfutago, 0xboxer"]\') }}'
  )
}}

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
  --marketId as market_id,
  questionId as question_id,
  if(outcome, 'yes', 'no') as outcome,
  evt_index,
  evt_tx_hash as tx_hash
from {{ source('polymarket_polygon', 'NegRiskAdapter_evt_OutcomeReported') }}
{% if is_incremental() %}
where {{ incremental_predicate('evt_block_time') }}
{% endif %}
