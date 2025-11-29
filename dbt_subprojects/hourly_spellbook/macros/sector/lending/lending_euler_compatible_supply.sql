{%
  macro lending_euler_v2_compatible_supply(
    blockchain,
    project,
    version,
    project_decoded_as = 'euler_v2',
    decoded_contract_name = 'EVault'
  )
%}

with 

src_EVault_evt_Deposit as (
  select *
  from {{ source(project_decoded_as ~ '_' ~ blockchain, decoded_contract_name ~ '_evt_Deposit') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
),

src_EVault_evt_Withdraw as (
  select *
  from {{ source(project_decoded_as ~ '_' ~ blockchain, decoded_contract_name ~ '_evt_Withdraw') }}
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


base_supply as (
  select
    'deposit' as transaction_type,
    cast(null as varbinary) as token_address,
    sender as depositor,
    owner as on_behalf_of,
    cast(null as varbinary) as withdrawn_to,
    cast(null as varbinary) as liquidator,
    cast(assets as double) as amount,
    contract_address,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_EVault_evt_Deposit 
  union all
  select
    'withdraw' as transaction_type,
    cast(null as varbinary) as token_address,
    owner as depositor,
    receiver as on_behalf_of,
    receiver as withdrawn_to,
    cast(null as varbinary) as liquidator,
    -1 * cast(assets as double) as amount,
    contract_address,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_EVault_evt_Withdraw
)

select
  '{{ blockchain }}' as blockchain,
  '{{ project }}' as project,
  '{{ version }}' as version,
  bs.transaction_type,
  ev.asset as token_address,
  bs.depositor,
  bs.on_behalf_of,
  bs.withdrawn_to,
  bs.liquidator,
  bs.amount,
  cast(date_trunc('month', bs.evt_block_time) as date) as block_month,
  bs.evt_block_time as block_time,
  bs.evt_block_number as block_number,
  bs.contract_address as project_contract_address,
  bs.evt_tx_hash as tx_hash,
  bs.evt_index
from base_supply bs 
inner join 
src_EVault_evt_EVaultCreated ev 
  on bs.contract_address = ev.contract_address

union all 

select 
  '{{ blockchain }}' as blockchain,
  '{{ project }}' as project,
  '{{ version }}' as version,
  'deposit_liquidation' as transaction_type,
  collateral as token_adddress,
  violator as depositor, 
  cast(null as varbinary) as on_behalf_of,
  liquidator as withdrawn_to,
  liquidator,
  -1 * cast(yieldBalance as double) as amount, 
  cast(date_trunc('month', evt_block_time) as date) as block_month,
  evt_block_time as block_time,
  evt_block_number as block_number,
  contract_address as project_contract_address,
  evt_tx_hash as tx_hash,
  evt_index
from 
src_EVault_evt_Liquidate
{% endmacro %}