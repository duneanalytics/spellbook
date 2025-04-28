{%
  macro lending_aave_v3_compatible_market(
    blockchain,
    project = 'aave',
    version = 'v3',
    project_decoded_as = 'aave_v3',
    decoded_contract_name = 'Pool'
  )
%}

with

reserve_data as (
  select
    r.evt_block_time as block_time,
    date_trunc('hour', r.evt_block_time) as block_hour,
    r.evt_block_number as block_number,
    r.reserve as token_address,
    t.symbol,
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
  block_hour,
  cast(date_trunc('month', block_time) as date) as block_month,
  block_number,
  token_address,
  symbol,
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

{# ######################################################################### #}

{%
  macro lending_aave_v3_compatible_market_hourly_agg(
    blockchain,
    project = 'aave',
    version = 'v3'
  )
%}

with

reserve_data as (
  select *
  from {{ ref('lending_' ~ blockchain ~ '_base_market') }}
  where blockchain = '{{ blockchain }}'
    and project = '{{ project }}'
    and version = '{{ version }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('block_time') }}
    {% endif %}
)

select
  blockchain,
  project,
  version,
  block_month,
  block_hour,
  token_address,
  symbol,
  max_by(liquidity_index, block_hour) as liquidity_index,
  max_by(variable_borrow_index, block_hour) as variable_borrow_index,
  avg(cast(deposit_rate as double)) / 1e27 as deposit_rate,
  avg(cast(stable_borrow_rate as double)) / 1e27 as stable_borrow_rate,
  avg(cast(variable_borrow_rate as double)) / 1e27 as variable_borrow_rate
from reserve_data
group by 1,2,3,4,5,6,7

{% endmacro %}
