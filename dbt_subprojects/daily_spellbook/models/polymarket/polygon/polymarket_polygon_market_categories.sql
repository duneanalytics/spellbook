{{
  config(
    schema = 'polymarket_polygon',
    alias = 'market_categories',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['market_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    post_hook = '{{ expose_spells(blockchains = \'["polygon"]\',
                                  spell_type = "project",
                                  spell_name = "polymarket",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

with markets as (
  select
    evt_block_time as block_time,
    evt_block_number as block_number,
    marketId as market_id,
    from_utf8(data) as data_decoded,
    oracle,
    feeBips as fee_bips,
    evt_index,
    evt_tx_hash as tx_hash
  from {{ source('polymarket_polygon', 'NegRiskAdapter_evt_MarketPrepared') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
)

select
  block_time,
  block_number,
  market_id,
  case
    when json_value(data_decoded, 'lax $.title' null on error) is not null
    then json_extract_scalar(data_decoded, '$.title')
    else regexp_extract(data_decoded, 'title:\s*(.*?),\s*description:', 1)
  end as market,
  case
    when json_value(data_decoded, 'lax $.description' null on error) is not null
    then json_extract_scalar(data_decoded, '$.description')
    else substr(data_decoded, strpos(data_decoded, 'description: ') + length('description: '))
  end as market_description,
  oracle,
  fee_bips, 
  evt_index,
  tx_hash
from markets
