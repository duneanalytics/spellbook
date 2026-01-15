{%- macro stablecoins_svm_transfers(
  blockchain,
  token_list,
  start_date = none
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
  t.amount as amount_raw,
  t.amount_display as amount,
  t.amount_usd,
  t.from_owner,
  t.to_owner,
  {{ solana_instruction_key('t.block_slot', 't.tx_index', 't.outer_instruction_index', 't.inner_instruction_index') }} as unique_key
from {{ source('tokens_' ~ blockchain, 'transfers') }} t
where 1 = 1
{% if start_date is not none %}
  and t.block_date >= date '{{ start_date }}'
{% endif %}
  and exists (
    select 1
    from {{ ref('tokens_' ~ blockchain ~ '_spl_stablecoins_' ~ token_list) }} s
    where s.token_mint_address = t.token_mint_address
  )
{% if is_incremental() %}
  and {{ incremental_predicate('t.block_date') }}
{% endif %}

{% endmacro %}
