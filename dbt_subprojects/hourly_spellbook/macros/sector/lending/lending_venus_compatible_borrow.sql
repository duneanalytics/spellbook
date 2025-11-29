{%
  macro lending_venus_compatible_borrow(
    blockchain,
    project,
    version,
    project_decoded_as = 'venus',
    decoded_contract_name = 'VToken'
  )
%}

with 

src_VToken_evt_Borrow as (
  select *
  from {{ source(project_decoded_as ~ '_' ~ blockchain, decoded_contract_name ~ '_evt_Borrow') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
),

src_VToken_evt_RepayBorrow as (
  select *
  from {{ source(project_decoded_as ~ '_' ~ blockchain, decoded_contract_name ~ '_evt_RepayBorrow') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
),

src_VToken_evt_LiquidateBorrow as (
  select *
  from {{ source(project_decoded_as ~ '_' ~ blockchain, decoded_contract_name ~ '_evt_LiquidateBorrow') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
),

base_borrow as (
  select
    'borrow' as transaction_type,
    cast(null as varchar) loan_type,
    cast(null as varbinary) as token_address,
    borrower as borrower,
    cast(null as varbinary) as on_behalf_of,
    cast(null as varbinary) as repayer,
    cast(null as varbinary) as liquidator,
    cast(borrowAmount as double) as amount,
    contract_address,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_VToken_evt_Borrow
  union all
  select
    'repay' as transaction_type,
    null as loan_type,
    cast(null as varbinary) as token_address,
    borrower,
    cast(null as varbinary) as on_behalf_of,
    repayer as repayer,
    cast(null as varbinary) as liquidator,
    -1 * cast(repayAmount as double) as amount,
    contract_address,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_VToken_evt_RepayBorrow
  union all
  select
    'borrow_liquidation' as transaction_type,
    null as loan_type,
    cast(null as varbinary) as token_address,
    borrower,
    cast(null as varbinary) as on_behalf_of,
    liquidator as repayer,
    liquidator as liquidator,
    -1 * cast(repayAmount as double) as amount,
    contract_address,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_VToken_evt_LiquidateBorrow
)

select
  '{{ blockchain }}' as blockchain,
  '{{ project }}' as project,
  '{{ version }}' as version,
  bb.transaction_type,
  bb.loan_type,
  vc.underlyingToken_address as token_address,
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
{{ ref( decoded_project ~ '_' ~ blockchain ~ '_ctokens' ) }} vc 
  on bb.contract_address = vc.vToken_contract_address

{% endmacro %}
