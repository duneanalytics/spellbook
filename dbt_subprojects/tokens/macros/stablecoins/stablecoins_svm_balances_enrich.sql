{%- macro stablecoins_svm_balances_enrich(
  base_balances,
  blockchain
) %}

select
  b.blockchain,
  b.day,
  b.address,
  b.token_mint_address as token_address,
  'spl_token' as token_standard,
  cast(null as uint256) as token_id,
  cast(b.token_balance * power(10, p.decimals) as uint256) as balance_raw,
  b.token_balance as balance,
  b.token_balance * p.price as balance_usd,
  b.last_updated
from {{ base_balances }} b
left join {{ source('prices_external', 'day') }} p
  on cast(b.day as timestamp) = p.timestamp
  and from_base58(b.token_mint_address) = p.contract_address
  {% if is_incremental() %}
  and {{ incremental_predicate('p.timestamp') }}
  {% endif %}
{% if is_incremental() %}
where {{ incremental_predicate('b.day') }}
{% endif %}

{% endmacro %}
