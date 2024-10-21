{{
  config(
    schema = 'polymarket_polygon',
    alias = 'markets',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['question_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    post_hook = '{{ expose_spells(blockchains = \'["polygon"]\',
                                  spell_type = "project",
                                  spell_name = "polymarket",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

with questions as (
  select
    evt_block_time as block_time,
    evt_block_number as block_number,
    'uma-v1' as source,
    cast(null as varbinary) as market_id,
    questionID as question_id,
    from_utf8(ancillaryData) as data_decoded,
    reward,
    rewardToken as reward_token,
    evt_index,
    evt_tx_hash as tx_hash
  from {{ source('minereum_polygon', 'UmaConditionalTokensBinaryAdapter_evt_QuestionInitialized') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
  
  union all
  
  select
    evt_block_time as block_time,
    evt_block_number as block_number,
    'uma-v2' as source,
    cast(null as varbinary) as market_id,
    questionID as question_id,
    from_utf8(ancillaryData) as data_decoded,
    reward,
    rewardToken as reward_token,
    evt_index,
    evt_tx_hash as tx_hash
  from {{ source('polymarket_polygon', 'UmaCtfAdapter_evt_QuestionInitialized') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}

  union all

  select
    evt_block_time as block_time,
    evt_block_number as block_number,
    'uma-v3' as source,
    cast(null as varbinary) as market_id,
    questionID as question_id,
    from_utf8(ancillaryData) as data_decoded,
    reward,
    rewardToken as reward_token,
    evt_index,
    evt_tx_hash as tx_hash
  from {{ source('polymarket_polygon', 'UmaCtfAdapter_v3_evt_QuestionInitialized') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}

  union all

  select
    evt_block_time as block_time,
    evt_block_number as block_number,
    'polymarket' as source,
    marketId as market_id,
    questionId as question_id,
    from_utf8(data) as data_decoded,
    cast(null as uint256) as reward,
    cast(null as varbinary) as reward_token,
    evt_index,
    evt_tx_hash as tx_hash
  from {{ source('polymarket_polygon', 'NegRiskAdapter_evt_QuestionPrepared') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
)

select
  block_time,
  block_number,
  source,
  market_id,
  question_id,
  case
    when coalesce(json_value(data_decoded, 'lax $.title' null on error), json_value(data_decoded, 'lax $.question' null on error)) is not null
    then coalesce(json_extract_scalar(data_decoded, '$.title'), json_extract_scalar(data_decoded, '$.question'))
    else regexp_extract(data_decoded, '(?i)(?:title|question):\s*(.*?),\s*description:', 1)
  end as question,
  case
    when json_value(data_decoded, 'lax $.description' null on error) is not null
    then json_extract_scalar(data_decoded, '$.description')
    else substr(data_decoded, strpos(data_decoded, 'description: ') + length('description: '))
  end as question_description,
  reward,
  reward_token,
  evt_index,
  tx_hash
from questions
