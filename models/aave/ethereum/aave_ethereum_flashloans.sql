{{
  config(
    schema = 'aave_ethereum',
    alias = 'flashloans',
    materialized = 'view',
    post_hook = '{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "aave",
                                    \'["hildobby", "tomfutago"]\') }}'
  )
}}

select
  blockchain,
  project,
  version,
  recipient,
  amount,
  usd_amount as amount_usd,
  fee,
  symbol as currency_symbol,
  token_address as currency_contract,
  contract_address,
  block_month,
  block_time,
  block_number,
  tx_hash,
  evt_index
from {{ ref('lending_flashloans') }}
where blockchain = 'ethereum'
  and project = 'aave'
