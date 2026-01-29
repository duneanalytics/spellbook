{% set chain = 'gnosis' %}

{{
  config(
    schema = 'stablecoins_' ~ chain,
    alias = 'transfers',
    materialized = 'view'
    , post_hook='{{ hide_spells() }}'
  )
}}

-- union of core and extended transfers with metadata enrichment

select
    t.blockchain
    , t.block_month
    , t.block_date
    , t.block_time
    , t.block_number
    , t.tx_hash
    , t.evt_index
    , t.trace_address
    , t.token_standard
    , t.token_address
    , t.token_symbol
    , m.backing as token_backing
    , m.name as token_name
    , t.amount_raw
    , t.amount
    , t.price_usd
    , t.amount_usd
    , t."from"
    , t."to"
    , t.unique_key
from {{ ref('stablecoins_' ~ chain ~ '_core_transfers') }} t
left join {{ ref('tokens_erc20_stablecoins_metadata') }} m
    on t.blockchain = m.blockchain
    and t.token_address = m.contract_address

union all

select
    t.blockchain
    , t.block_month
    , t.block_date
    , t.block_time
    , t.block_number
    , t.tx_hash
    , t.evt_index
    , t.trace_address
    , t.token_standard
    , t.token_address
    , t.token_symbol
    , m.backing as token_backing
    , m.name as token_name
    , t.amount_raw
    , t.amount
    , t.price_usd
    , t.amount_usd
    , t."from"
    , t."to"
    , t.unique_key
from {{ ref('stablecoins_' ~ chain ~ '_extended_transfers') }} t
left join {{ ref('tokens_erc20_stablecoins_metadata') }} m
    on t.blockchain = m.blockchain
    and t.token_address = m.contract_address
