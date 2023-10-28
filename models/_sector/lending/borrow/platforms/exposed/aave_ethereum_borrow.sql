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
  token_address,
  borrower,
  repayer,
  liquidator,
  amount,
  evt_tx_hash,
  evt_index,
  evt_block_month,
  evt_block_time,
  evt_block_number
from {{ ref('lending_borrow') }}
where blockchain = 'ethereum'
  and project = 'aave'
