{%
  macro lending_aave_v3_compatible_market(
    blockchain,
    project = 'aave',
    version = '3',
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
    version = '3'
  )
%}

with

reserve_data_base as (
  select * from {{ ref(project ~ '_v' ~ version ~ '_' ~ blockchain ~ '_base_market') }}
),

reserve_data_hourly_agg as (
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
  from reserve_data_base
  group by 1,2,3,4,5,6,7
),

reserve_data_hourly_changes as (
  select
    *,
    lead(block_hour) over (partition by token_address order by block_hour) as next_update_block_hour
  from (
    -- straight up incremental
    select * from reserve_data_hourly_agg
    {% if is_incremental() %}
    where {{ incremental_predicate('block_hour') }}
    {% endif %}
    -- retrieve last known hourly agg update from before the current window to correctly populate the forward fill
    {% if is_incremental() %}
    union all
    select
      blockchain,
      project,
      version,
      max(block_month) as block_month,
      max(block_hour) as block_hour,
      token_address,
      symbol,
      max(liquidity_index) as liquidity_index,
      max(variable_borrow_index) as variable_borrow_index,
      max(deposit_rate) as deposit_rate,
      max(stable_borrow_rate) as stable_borrow_rate,
      max(variable_borrow_rate) as variable_borrow_rate
    from reserve_data_hourly_agg
    where not {{ incremental_predicate('block_hour') }}
    group by 1,2,3,6,7
    {% endif %}
  ) t
),

reserve_token_start as (
  select
    blockchain,
    project,
    version,
    token_address,
    min(block_hour) as block_hour_start
  from reserve_data_hourly_changes
  group by 1,2,3,4
),

token_hourly_sequence as (
  select
    rts.blockchain,
    rts.project,
    rts.version,
    rts.token_address,
    h.timestamp as block_hour
  from reserve_token_start rts
    inner join {{ source('utils', 'hours') }} h on rts.block_hour_start <= h.timestamp
),

forward_fill as (
  select
    ths.blockchain,
    ths.project,
    ths.version,
    cast(date_trunc('month', ths.block_hour) as date) as block_month,
    ths.block_hour,
    ths.token_address,
    rdhc.symbol,
    rdhc.liquidity_index,
    rdhc.variable_borrow_index,
    rdhc.deposit_rate,
    rdhc.stable_borrow_rate,
    rdhc.variable_borrow_rate
  from token_hourly_sequence ths
    left join reserve_data_hourly_changes rdhc
      on ths.blockchain = rdhc.blockchain
      and ths.project = rdhc.project
      and ths.version = rdhc.version
      and ths.token_address = rdhc.token_address
      and ths.block_hour >= rdhc.block_hour
      and (ths.block_hour < rdhc.next_update_block_hour or rdhc.next_update_block_hour is null)
)

select *
from forward_fill
{% if is_incremental() %}
where {{ incremental_predicate('block_hour') }}
{% endif %}

{% endmacro %}
