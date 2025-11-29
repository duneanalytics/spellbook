{%
  macro lending_euler_v2_compatible_borrow(
    blockchain,
    project,
    version,
    project_decoded_as = 'euler_v2',
    decoded_contract_name = 'EVault'
  )
%}

with 

src_EVault_evt_Borrow as (
  select *
  from {{ source(project_decoded_as ~ '_' ~ blockchain, decoded_contract_name ~ '_evt_Borrow') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
),

src_EVault_evt_Repay as (
  select *
  from {{ source(project_decoded_as ~ '_' ~ blockchain, decoded_contract_name ~ '_evt_Repay') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
),

src_EVault_evt_Liquidate as (
  select *
  from {{ source(project_decoded_as ~ '_' ~ blockchain, decoded_contract_name ~ '_evt_Liquidate') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
),

src_EVault_evt_EVaultCreated as (
  select *
  from {{ source(project_decoded_as ~ '_' ~ blockchain, decoded_contract_name ~ '_evt_EVaultCreated') }}
),

base_borrow as (
  select
    'borrow' as transaction_type,
    cast(null as varchar) loan_type,
    cast(null as varbinary) as token_address,
    account as borrower,
    account as on_behalf_of,
    cast(null as varbinary) as repayer,
    cast(null as varbinary) as liquidator,
    cast(assets as double) as amount,
    contract_address,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_EVault_evt_Borrow
  union all
  select
    'repay' as transaction_type,
    cast(null as varchar) loan_type,
    cast(null as varbinary) as token_address,
    account as borrower,
    cast(null as varbinary) as on_behalf_of,
    account as repayer,
    cast(null as varbinary) as liquidator,
    -1 * cast(assets as double) as amount,
    contract_address,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_EVault_evt_Repay
  union all
  select
    'borrow_liquidation' as transaction_type,
    cast(null as varchar) loan_type,
    cast(null as varbinary) as token_address,
    violator as borrower,
    cast(null as varbinary) as on_behalf_of,
    liquidator as repayer,
    liquidator as liquidator,
    -1 * cast(repayAssets as double) as amount,
    contract_address,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_EVault_evt_Liquidate
)

select
  '{{ blockchain }}' as blockchain,
  '{{ project }}' as project,
  '{{ version }}' as version,
  bb.transaction_type,
  bb.loan_type,
  ev.asset as token_address,
  bb.borrower,
  bb.on_behalf_of,
  bb.repayer,
  bb.liquidator,
  bb.amount,
  cast(date_trunc('month', bb.evt_block_time) as date) as block_month,
  bb.evt_block_time as block_time,
  bb.evt_block_number as block_number,
  bb.contract_address as project_contract_address,
  bb.evt_tx_hash as tx_hash,
  bb.evt_index
from base_borrow bb 
inner join 
src_EVault_evt_EVaultCreated ev 
  on bb.contract_address = ev.contract_address

{% endmacro %}