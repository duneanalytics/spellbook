{% set chain = 'solana' %}

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
  t.blockchain,
  t.block_month,
  t.block_date,
  t.block_time,
  t.block_slot,
  t.tx_id,
  t.tx_index,
  t.outer_instruction_index,
  t.inner_instruction_index,
  t.token_version,
  t.token_mint_address,
  t.token_symbol,
  m.backing as token_backing,
  m.name as token_name,
  t.currency,
  t.amount_raw,
  t.amount,
  t.amount_usd,
  t.price_usd,
  t.from_owner,
  t.to_owner,
  t.from_token_account,
  t.to_token_account,
  t.tx_signer,
  t.outer_executing_account,
  t.action,
  t.unique_key
from {{ ref('stablecoins_' ~ chain ~ '_core_transfers') }} t
left join {{ ref('tokens_spl_stablecoins_metadata') }} m
  on t.blockchain = m.blockchain
  and t.token_mint_address = m.token_mint_address
union all
select
  t.blockchain,
  t.block_month,
  t.block_date,
  t.block_time,
  t.block_slot,
  t.tx_id,
  t.tx_index,
  t.outer_instruction_index,
  t.inner_instruction_index,
  t.token_version,
  t.token_mint_address,
  t.token_symbol,
  m.backing as token_backing,
  m.name as token_name,
  t.currency,
  t.amount_raw,
  t.amount,
  t.amount_usd,
  t.price_usd,
  t.from_owner,
  t.to_owner,
  t.from_token_account,
  t.to_token_account,
  t.tx_signer,
  t.outer_executing_account,
  t.action,
  t.unique_key
from {{ ref('stablecoins_' ~ chain ~ '_extended_transfers') }} t
left join {{ ref('tokens_spl_stablecoins_metadata') }} m
  on t.blockchain = m.blockchain
  and t.token_mint_address = m.token_mint_address
