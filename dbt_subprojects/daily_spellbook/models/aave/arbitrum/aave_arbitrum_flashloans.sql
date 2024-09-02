{{
  config(
    schema = 'aave_arbitrum',
    alias = 'flashloans',
    materialized = 'view'
  )
}}

select
  blockchain,
  project,
  version,
  recipient,
  amount,
  amount_usd,
  fee,
  symbol as currency_symbol,
  token_address as currency_contract,
  project_contract_address as contract_address,
  block_month,
  block_time,
  block_number,
  tx_hash,
  evt_index
from {{ source('lending','flashloans') }}
where blockchain = 'arbitrum'
  and project = 'aave'
