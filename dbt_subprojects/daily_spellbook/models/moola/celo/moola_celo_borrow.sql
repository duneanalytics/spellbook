{{
  config(
    schema = 'moola_celo',
    alias = 'borrow',
    materialized = 'view',
    post_hook = '{{ expose_spells(\'["celo"]\',
                                    "project",
                                    "moola",
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
  amount_usd,
  block_month,
  block_time,
  block_number,
  tx_hash,
  evt_index
from {{ source('lending','borrow') }}
where blockchain = 'celo'
  and project = 'moola'
