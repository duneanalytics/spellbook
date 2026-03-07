{%- macro stablecoins_svm_balances_enrich(
  base_balances,
  blockchain
) %}

select
  b.blockchain,
  b.day,
  b.address,
  f.symbol as token_symbol,
  b.token_mint_address as token_address,
  'spl_token' as token_standard,
  cast(null as uint256) as token_id,
  s.currency,
  b.balance_raw,
  cast(b.balance_raw as double) / power(10, f.decimals) as balance,
  cast(b.balance_raw as double) / power(10, f.decimals) * fx.exchange_rate as balance_usd,
  b.last_updated
from {{ base_balances }} b
inner join {{ ref('tokens_' ~ blockchain ~ '_spl_stablecoins') }} s
  on s.token_mint_address = b.token_mint_address
left join {{ source('tokens_solana', 'fungible') }} f
  on b.token_mint_address = f.token_mint_address
left join {{ source('prices', 'fx_exchange_rates') }} fx
  on fx.base_currency = s.currency
  and fx.target_currency = 'USD'
  and fx.date = b.day
  {% if is_incremental() %}
  and {{ incremental_predicate('fx.date') }}
  {% endif %}
{% if is_incremental() %}
where {{ incremental_predicate('b.day') }}
{% endif %}

{% endmacro %}
