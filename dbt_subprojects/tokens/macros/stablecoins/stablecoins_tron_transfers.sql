{%- macro stablecoins_tron_transfers(token_list) -%}

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
  t.contract_address as token_address,
  t.symbol as token_symbol,
  s.currency,
  t.amount_raw,
  t.amount,
  t.amount * fx.exchange_rate as amount_usd,
  fx.exchange_rate as price_usd,
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
from {{ ref('tokens_tron_transfers') }} t
inner join {{ ref('tokens_tron_trc20_stablecoins_' ~ token_list) }} s
  on t.contract_address_varchar = s.contract_address
left join {{ source('prices', 'fx_exchange_rates') }} fx
  on fx.base_currency = s.currency
  and fx.target_currency = 'USD'
  and fx.date = t.block_date
{% if is_incremental() %}
where {{ incremental_predicate('t.block_date') }}
{% endif %}

{%- endmacro -%}
