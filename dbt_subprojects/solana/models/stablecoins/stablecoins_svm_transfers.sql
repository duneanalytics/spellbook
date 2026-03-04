{% set chains = [
  'solana'
] %}

{{
  config(
    schema = 'stablecoins_svm',
    alias = 'transfers',
    materialized = 'view'
    , post_hook='{{ hide_spells() }}'
  )
}}

select *
from (
  {% for chain in chains %}
  select
    blockchain,
    block_month,
    block_date,
    block_time,
    block_slot,
    tx_id,
    tx_index,
    outer_instruction_index,
    inner_instruction_index,
    token_version,
    token_mint_address,
    token_symbol,
    token_backing,
    token_name,
    currency,
    amount_raw,
    amount,
    amount_usd,
    price_usd,
    from_owner,
    to_owner,
    from_token_account,
    to_token_account,
    tx_signer,
    outer_executing_account,
    action,
    unique_key
  from {{ ref('stablecoins_' ~ chain ~ '_transfers') }}
  {% if not loop.last %}
  union all
  {% endif %}
  {% endfor %}
)
