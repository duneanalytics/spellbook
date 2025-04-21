{%
  macro lending_aave_v3_compatible_reserve(
    blockchain,
    project,
    version,
    project_decoded_as = 'aave_v3',
    decoded_contract_name = 'Pool'
  )
%}

with

reserve_data as (
  select
    r.evt_block_time as block_time,
    r.evt_block_date as block_date,
    r.evt_block_number as block_number,
    r.reserve as token_address,
    t.symbol as token_symbol,
    r.liquidityIndex as liquidity_index,
    r.variableBorrowIndex as variable_borrow_index,
    r.liquidityRate as deposit_rate,
    r.stableBorrowRate as stable_borrow_rate,
    r.variableBorrowRate as variable_borrow_rate,
    r.contract_address as project_contract_address,
    r.evt_index,
    r.evt_tx_hash as tx_hash
  from {{ source(project_decoded_as ~ '_' ~ blockchain, decoded_contract_name ~ '_evt_ReserveDataUpdated') }} r
    left join {{ source('tokens', 'erc20') }} as t on r.reserve = t.contract_address
  where t.blockchain = '{{ blockchain }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('r.evt_block_time') }}
    {% endif %}
)

select
  '{{ blockchain }}' as blockchain,
  '{{ project }}' as project,
  '{{ version }}' as version,
  block_time,
  block_date,
  cast(date_trunc('month', block_date) as date) as block_month,
  block_number,
  token_address,
  token_symbol,
  liquidity_index,
  variable_borrow_index,
  deposit_rate,
  stable_borrow_rate,
  variable_borrow_rate,
  project_contract_address,
  evt_index,
  tx_hash
from reserve_data

{% endmacro %}
