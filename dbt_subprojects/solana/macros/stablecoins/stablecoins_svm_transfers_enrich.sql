{%- macro stablecoins_svm_transfers_enrich(
  base_transfers,
  blockchain
) %}

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
  s.currency,
  t.amount_raw,
  t.amount,
  t.amount * fx.exchange_rate as amount_usd,
  fx.exchange_rate as price_usd,
  t.from_owner,
  t.to_owner,
  t.from_token_account,
  t.to_token_account,
  t.tx_signer,
  t.outer_executing_account,
  t.action,
  t.unique_key
from {{ base_transfers }} t
inner join {{ ref('tokens_' ~ blockchain ~ '_spl_stablecoins') }} s
  on s.token_mint_address = t.token_mint_address
left join {{ ref('tokens_spl_stablecoins_metadata') }} m
  on t.blockchain = m.blockchain
  and t.token_mint_address = m.token_mint_address
left join {{ source('prices', 'fx_exchange_rates') }} fx
  on fx.base_currency = s.currency
  and fx.target_currency = 'USD'
  and fx.date = t.block_date

{% endmacro %}
