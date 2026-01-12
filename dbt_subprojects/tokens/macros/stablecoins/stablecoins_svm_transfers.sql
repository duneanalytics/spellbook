{%- macro stablecoins_svm_transfers(
  blockchain,
  token_list
) %}

with stablecoin_tokens as (
  select token_mint_address
  from {{ ref('tokens_' ~ blockchain ~ '_spl_stablecoins_' ~ token_list) }}
),

prices as (
  select
    contract_address,
    minute,
    price,
    decimals,
    symbol
  from {{ source('prices', 'usd_forward_fill') }}
  where blockchain = '{{blockchain}}'
  and minute >= timestamp '2020-10-02 00:00' -- solana start date
  {% if is_incremental() %}
  and {{ incremental_predicate('minute') }}
  {% endif %}
)

select
  '{{blockchain}}' as blockchain,
  cast(date_trunc('month', t.block_date) as date) as block_month,
  t.block_date,
  t.block_time,
  t.block_slot as block_number,
  t.tx_id as tx_hash,
  t.outer_instruction_index as evt_index,
  cast(coalesce(t.inner_instruction_index, 0) as array(bigint)) as trace_address,
  t.token_version as token_standard,
  t.token_mint_address as token_address,
  p.symbol as token_symbol,
  t.amount as amount_raw,
  case
    when p.decimals is null then null
    when p.decimals = 0 then cast(t.amount as double)
    else cast(t.amount as double) / power(10, p.decimals)
  end as amount,
  p.price as price_usd,
  case
    when p.decimals is null then null
    when p.decimals = 0 then p.price * cast(t.amount as double)
    else p.price * cast(t.amount as double) / power(10, p.decimals)
  end as amount_usd,
  t.from_owner as "from",
  t.to_owner as "to",
  {{ dbt_utils.generate_surrogate_key(['t.tx_id', 't.outer_instruction_index', 't.inner_instruction_index', 't.token_mint_address']) }} as unique_key
from {{ source('tokens_solana', 'transfers') }} t
inner join stablecoin_tokens s
  on t.token_mint_address = s.token_mint_address
left join prices p
  on p.contract_address = from_base58(t.token_mint_address)
  and p.minute = date_trunc('minute', t.block_time)
where t.action = 'transfer'
{% if is_incremental() %}
  and {{ incremental_predicate('t.block_date') }}
{% endif %}

{% endmacro %}
