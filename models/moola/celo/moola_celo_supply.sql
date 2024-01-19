{{
  config(
    schema = 'moola_celo',
    alias = 'supply',
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
  symbol,
  token_address,
  depositor,
  withdrawn_to,
  liquidator,
  amount,
  usd_amount,
  block_month,
  block_time,
  block_number,
  tx_hash,
  evt_index
from {{ ref('lending_supply') }}
where blockchain = 'celo'
  and project = 'moola'
