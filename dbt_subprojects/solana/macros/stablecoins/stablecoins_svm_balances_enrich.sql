{%- macro stablecoins_svm_balances_enrich(
  base_balances,
  blockchain
) %}

select
  b.blockchain,
  b.day,
  b.address,
  p.symbol as token_symbol,
  b.token_mint_address as token_address,
  'spl_token' as token_standard,
  cast(null as uint256) as token_id,
  b.balance_raw,
  cast(b.balance_raw as double) / power(10, p.decimals) as balance,
  cast(b.balance_raw as double) / power(10, p.decimals) * p.price as balance_usd,
  b.last_updated
from {{ base_balances }} b
left join {{ source('prices_external', 'day') }} p
  on cast(b.day as timestamp) = p.timestamp
  and from_base58(b.token_mint_address) = p.contract_address
  and p.blockchain = '{{ blockchain }}'
  {% if is_incremental() %}
  and {{ incremental_predicate('p.timestamp') }}
  {% endif %}
{% if is_incremental() %}
where {{ incremental_predicate('b.day') }}
{% endif %}

{% endmacro %}
