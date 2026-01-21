{%
  macro lending_venus_compatible_supply(
    blockchain,
    project,
    version,
    project_decoded_as = 'venus',
    decoded_contract_name = 'VToken'
  )
%}

with 

src_VToken_evt_Mint as (
  select *
  from {{ source(project_decoded_as ~ '_' ~ blockchain, decoded_contract_name ~ '_evt_Mint') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
),

src_VToken_evt_Redeem as (
  select *
  from {{ source(project_decoded_as ~ '_' ~ blockchain, decoded_contract_name ~ '_evt_Redeem') }}
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

base_supply as (
  select
    'deposit' as transaction_type,
    cast(null as varbinary) as token_address,
    minter as depositor,
    cast(null as varbinary) as on_behalf_of,
    cast(null as varbinary) as withdrawn_to,
    cast(null as varbinary) as liquidator,
    cast(mintAmount as double) as amount,
    mintAmount as amount_raw,
    contract_address,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_VToken_evt_Mint
  union all
  select
    'withdraw' as transaction_type,
    cast(null as varbinary) as token_address,
    redeemer as depositor,
    cast(null as varbinary) as on_behalf_of,
    redeemer as withdrawn_to,
    cast(null as varbinary) as liquidator,
    -1 * cast(redeemAmount as double) as amount,
    redeemAmount as amount_raw,
    contract_address,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_VToken_evt_Redeem
)

select
  '{{ blockchain }}' as blockchain,
  '{{ project }}' as project,
  '{{ version }}' as version,
  bb.transaction_type,
  vc.underlyingToken_address as token_address,
  bb.depositor,
  bb.on_behalf_of,
  bb.withdrawn_to,
  bb.liquidator,
  bb.amount,
  bb.amount_raw,
  cast(date_trunc('month', bb.evt_block_time) as date) as block_month,
  bb.evt_block_time as block_time,
  bb.evt_block_number as block_number,
  bb.contract_address as project_contract_address,
  bb.evt_tx_hash as tx_hash,
  bb.evt_index
from base_supply bb 
inner join 
{{ ref( project_decoded_as ~ '_' ~ blockchain ~ '_ctokens' ) }} vc 
  on bb.contract_address = vc.vToken_contract_address

union all 

select
  '{{ blockchain }}' as blockchain,
  '{{ project }}' as project,
  '{{ version }}' as version,
  'deposit_liquidation' as transaction_type,
  bb.vTokenCollateral as token_address,
  bb.borrower as depositor,
  cast(null as varbinary) as on_behalf_of,
  bb.liquidator as withdrawn_to,
  bb.liquidator,
  -1 * cast(seizetokens as double) as amount,
  seizetokens as amount_raw,
  cast(date_trunc('month', bb.evt_block_time) as date) as block_month,
  bb.evt_block_time as block_time,
  bb.evt_block_number as block_number,
  bb.contract_address as project_contract_address,
  bb.evt_tx_hash as tx_hash,
  bb.evt_index
from src_VToken_evt_LiquidateBorrow bb

{% endmacro %}
