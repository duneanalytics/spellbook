{{
  config(
    schema = 'aave_ethereum',
    alias = 'borrow',
    materialized = 'view',
    post_hook = '{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "aave",
                                    \'["tomfutago"]\') }}'
  )
}}

select
  blockchain,
  project,
  version,
  transaction_type,
  loan_type,
  symbol,
  token_address,
  borrower,
  on_behalf_of,
  repayer,
  liquidator,
  amount,
  amount_usd,
  block_month,
  block_time,
  block_number,
  project_contract_address,
  tx_hash,
  evt_index
from {{ source('lending','borrow') }}
where blockchain = 'ethereum'
  and project = 'aave'
