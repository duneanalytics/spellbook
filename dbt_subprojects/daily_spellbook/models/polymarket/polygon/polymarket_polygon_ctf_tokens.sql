{{
  config(
    schema = 'polymarket_polygon',
    alias = 'ctf_tokens',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['condition_id','token0','token1'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

with ctf_tokens as (
  select
    evt_block_time as block_time,
    evt_block_number as block_number,
    conditionId as condition_id,
    token0,
    token1,
    evt_index,
    evt_tx_hash as tx_hash
  from {{ source('polymarket_polygon', 'CTFExchange_evt_TokenRegistered') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
  union all
  select
    evt_block_time as block_time,
    evt_block_number as block_number,
    conditionId as condition_id,
    token0,
    token1,
    evt_index,
    evt_tx_hash as tx_hash
  from {{ source('polymarket_polygon', 'NegRiskCtfExchange_evt_TokenRegistered') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
)

select
  t.block_time,
  t.block_number,
  t.condition_id,
  t.token0,
  t.token1,
  t.evt_index,
  t.tx_hash
from ctf_tokens t
  inner join {{ ref('polymarket_polygon_market_conditions') }} mc on t.condition_id = mc.condition_id
