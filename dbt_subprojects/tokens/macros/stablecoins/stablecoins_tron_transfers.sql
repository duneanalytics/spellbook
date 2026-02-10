{%- macro stablecoins_tron_transfers(token_list) -%}

with stablecoin_tokens as (
  select contract_address as token_address
  from {{ ref('tokens_tron_trc20_stablecoins_' ~ token_list) }}
)

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
  s.token_address as token_address_varchar,
  t.symbol as token_symbol,
  t.amount_raw,
  t.amount,
  t.price_usd,
  t.amount_usd,
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
inner join stablecoin_tokens s
  on t.contract_address_varchar = s.token_address
{% if is_incremental() %}
where {{ incremental_predicate('t.block_date') }}
{% endif %}

{%- endmacro -%}
