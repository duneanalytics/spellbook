{%- macro stablecoins_svm_transfers(
  blockchain,
  token_list,
  start_date=none
) %}

select
  '{{blockchain}}' as blockchain,
  cast(date_trunc('month', t.block_date) as date) as block_month,
  t.block_date,
  t.block_time,
  t.block_slot as block_number,
  t.tx_id as tx_hash,
  t.outer_instruction_index as evt_index,
  array[cast(coalesce(t.inner_instruction_index, 0) as bigint)] as trace_address,
  t.token_version as token_standard,
  t.token_mint_address as token_address,
  t.symbol as token_symbol,
  t.amount as amount_raw,
  t.amount_display as amount,
  t.amount_usd,
  t.from_owner as "from",
  t.to_owner as "to",
  (
    cast(t.block_slot as decimal(38,0)) * cast(1e21 as decimal(38,0))
    + cast(t.tx_index as decimal(38,0)) * cast(1e12 as decimal(38,0))
    + cast(coalesce(t.outer_instruction_index, 0) as decimal(38,0)) * cast(1e6 as decimal(38,0))
    + cast(coalesce(t.inner_instruction_index, 0) as decimal(38,0))
  ) as unique_key
from {{ source('tokens_' ~ blockchain, 'transfers') }} t
where t.action = 'transfer'
  and exists (
    select 1
    from {{ ref('tokens_' ~ blockchain ~ '_spl_stablecoins_' ~ token_list) }} s
    where s.token_mint_address = t.token_mint_address
  )
{% if start_date is not none %}
  and t.block_date >= date '{{ start_date }}'
{% endif %}
{% if is_incremental() %}
  and {{ incremental_predicate('t.block_date') }}
{% endif %}

{% endmacro %}
