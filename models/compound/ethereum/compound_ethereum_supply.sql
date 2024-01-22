{{
  config(
    schema = 'compound_ethereum',
    alias = 'supply',
    materialized = 'view',
    post_hook = '{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "compound",
                                    \'["bizzyvinci", "hosuke", "tomfutago"]\') }}'
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
  supplyer,
  repayer,
  liquidator,
  amount,
  usd_amount,
  block_month,
  block_time,
  block_number,
  tx_hash,
  evt_index
from {{ ref('lending_supply') }}
where blockchain = 'ethereum'
  and project = 'compound'
