{{
  config(
    schema = 'polymarket_polygon',
    alias = 'base_ctf_tokens',
    materialized = 'view'
  )
}}

with ctf_tokens as (
  select
    *,
    row_number() over (partition by condition_id, token0, token1 order by block_time) as rn
  from (
    select
      evt_block_time as block_time,
      evt_block_number as block_number,
      conditionId as condition_id,
      token0,
      token1,
      evt_index,
      evt_tx_hash as tx_hash
    from {{ source('polymarket_polygon', 'CTFExchange_evt_TokenRegistered') }}
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
  ) t
)

select
  block_time,
  block_number,
  condition_id,
  token0,
  token1,
  evt_index,
  tx_hash
from ctf_tokens
where rn = 1
