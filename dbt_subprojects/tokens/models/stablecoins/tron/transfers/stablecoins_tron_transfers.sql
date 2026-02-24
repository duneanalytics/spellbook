{% set chain = 'tron' %}

{{
  config(
    schema = 'stablecoins_' ~ chain,
    alias = 'transfers',
    materialized = 'view',
    post_hook = '{{ expose_spells(\'["tron"]\',
        "sector",
        "stablecoins",
        \'["tomfutago"]\') }}'
  )
}}

-- union of core and extended transfers; columns = EVM macro + tron extras + metadata

select
  t.blockchain,
  t.block_month,
  t.block_date,
  t.block_time,
  t.block_number,
  t.tx_hash,
  t.evt_index,
  t.trace_address,
  t.token_standard,
  t.token_address,
  t.token_symbol,
  t.currency,
  t.amount_raw,
  t.amount,
  t.amount_usd,
  t.price_usd,
  t."from",
  t."to",
  t.unique_key,
  t.tx_from,
  t.tx_to,
  t.tx_index,
  t.contract_address,
  t.tx_hash_varchar,
  t.contract_address_varchar,
  t.from_varchar,
  t.to_varchar,
  t.tx_from_varchar,
  t.tx_to_varchar
from {{ ref('stablecoins_' ~ chain ~ '_core_transfers') }} t

union all

select
  t.blockchain,
  t.block_month,
  t.block_date,
  t.block_time,
  t.block_number,
  t.tx_hash,
  t.evt_index,
  t.trace_address,
  t.token_standard,
  t.token_address,
  t.token_symbol,
  t.currency,
  t.amount_raw,
  t.amount,
  t.amount_usd,
  t.price_usd,
  t."from",
  t."to",
  t.unique_key,
  t.tx_from,
  t.tx_to,
  t.tx_index,
  t.contract_address,
  t.tx_hash_varchar,
  t.contract_address_varchar,
  t.from_varchar,
  t.to_varchar,
  t.tx_from_varchar,
  t.tx_to_varchar
from {{ ref('stablecoins_' ~ chain ~ '_extended_transfers') }} t
