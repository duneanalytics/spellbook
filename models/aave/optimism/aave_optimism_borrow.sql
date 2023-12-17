{{
  config(
    schema = 'aave_optimism',
    alias = 'borrow',
    materialized = 'view',
    post_hook = '{{ expose_spells(\'["optimism"]\',
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
  repayer,
  liquidator,
  amount,
  usd_amount,
  block_month,
  block_time,
  block_number,
  tx_hash,
  evt_index
from {{ ref('lending_borrow') }}
where blockchain = 'optimism'
  and project = 'aave'
