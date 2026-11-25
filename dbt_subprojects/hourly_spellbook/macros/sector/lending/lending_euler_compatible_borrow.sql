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

-- A liquidation moves the violator's debt to the liquidator, and the vault logs that move as Repay for
-- the violator and Borrow for the liquidator, alongside Liquidate. Pair each liquidation with the last
-- matching Repay before it, so that the violator's debt reduction is reported once, as
-- borrow_liquidation, and not a second time as repay. The liquidator's Borrow row is kept: it is the
-- debt the liquidator took on.
liquidation_repays as (
  select
    l.contract_address,
    l.evt_tx_hash,
    max(r.evt_index) as evt_index
  from src_EVault_evt_Liquidate l
    inner join src_EVault_evt_Repay r
      on r.evt_block_number = l.evt_block_number
      and r.evt_tx_hash = l.evt_tx_hash
      and r.contract_address = l.contract_address
      and r.account = l.violator
      and r.assets = l.repayAssets
      and r.evt_index < l.evt_index
  group by l.contract_address, l.evt_tx_hash, l.evt_index
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
    assets as amount_raw,
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
    r.account as borrower,
    cast(null as varbinary) as on_behalf_of,
    r.account as repayer,
    cast(null as varbinary) as liquidator,
    -1 * cast(r.assets as double) as amount,
    r.assets as amount_raw,
    r.contract_address,
    r.evt_tx_hash,
    r.evt_index,
    r.evt_block_time,
    r.evt_block_number
  from src_EVault_evt_Repay r
    left join liquidation_repays lr
      on lr.evt_tx_hash = r.evt_tx_hash
      and lr.contract_address = r.contract_address
      and lr.evt_index = r.evt_index
  where lr.evt_index is null
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
    repayAssets as amount_raw,
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
  bb.amount_raw,
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