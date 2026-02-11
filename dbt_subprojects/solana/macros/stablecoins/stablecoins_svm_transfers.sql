{%- macro stablecoins_svm_transfers(
  blockchain,
  token_list
) %}

select
  '{{blockchain}}' as blockchain,
  cast(date_trunc('month', t.block_date) as date) as block_month,
  t.block_date,
  t.block_time,
  t.block_slot,
  t.tx_id,
  t.tx_index,
  t.outer_instruction_index,
  coalesce(t.inner_instruction_index, 0) as inner_instruction_index,
  t.token_version,
  t.token_mint_address,
  t.symbol as token_symbol,
  s.currency,
  t.amount as amount_raw,
  t.amount_display as amount,
  t.amount_display * fx.exchange_rate as amount_usd,
  fx.exchange_rate as price_usd,
  t.from_owner,
  t.to_owner,
  t.from_token_account,
  t.to_token_account,
  t.tx_signer,
  t.outer_executing_account,
  t.action,
  {{ solana_instruction_key('t.block_slot', 't.tx_index', 't.outer_instruction_index', 't.inner_instruction_index') }} as unique_key
from {{ source('tokens_' ~ blockchain, 'transfers') }} t
inner join {{ ref('tokens_' ~ blockchain ~ '_spl_stablecoins_' ~ token_list) }} s
  on s.token_mint_address = t.token_mint_address
left join {{ source('prices', 'fx_exchange_rates') }} fx
  on fx.base_currency = s.currency
  and fx.target_currency = 'USD'
  and fx.date = t.block_date
{# microbatch auto-filters via event_time; no is_incremental() block needed #}

{% endmacro %}
