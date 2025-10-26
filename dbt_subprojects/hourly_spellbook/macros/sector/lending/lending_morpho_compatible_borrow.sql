{%
  macro lending_morpho_v1_compatible_borrow(
    blockchain = '',
    project = 'morpho',
    version = '1',
    decoded_project = 'morpho_blue'
  )
%}

with

markets as (
  select 
    id,
    from_hex(json_extract_scalar("marketParams", '$.loanToken')) as loanToken,
    from_hex(json_extract_scalar("marketParams", '$.collateralToken')) as collateralToken,
    from_hex(json_extract_scalar("marketParams", '$.oracle')) as oracle,
    json_extract_scalar("marketParams", '$.irm') as irm,
    json_extract_scalar("marketParams", '$.lltv') as lltv
  from {{ source(decoded_project ~ '_' ~ blockchain, 'MorphoBlue_evt_CreateMarket') }}
),

src_MorphoBlue_evt_Borrow as (
  select *
  from {{ source(decoded_project ~ '_' ~ blockchain, 'MorphoBlue_evt_Borrow') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
),

src_MorphoBlue_evt_Repay as (
  select *
  from {{ source(decoded_project ~ '_' ~ blockchain, 'MorphoBlue_evt_Repay') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
),

src_MorphoBlue_evt_Liquidate as (
  select *
  from {{ source(decoded_project ~ '_' ~ blockchain, 'MorphoBlue_evt_Liquidate') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
),

base_borrow as (
  select
    'borrow' as transaction_type,
    'variable' as loan_type,
    m.loanToken as token_address,
    b.caller as borrower,
    b.onBehalf as on_behalf_of,
    cast(null as varbinary) as repayer,
    cast(null as varbinary) as liquidator,
    cast(b.assets as double) as amount,
    b.contract_address,
    b.evt_tx_hash,
    b.evt_index,
    b.evt_block_time,
    b.evt_block_number
  from src_MorphoBlue_evt_Borrow b
    inner join markets m on b.id = m.id
  union all
  select
    'repay' as transaction_type,
    null as loan_type,
    m.loanToken as token_address,
    r.caller as borrower,
    r.onBehalf as on_behalf_of,
    r.caller as repayer,
    cast(null as varbinary) as liquidator,
    -1 * cast(r.assets as double) as amount,
    r.contract_address,
    r.evt_tx_hash,
    r.evt_index,
    r.evt_block_time,
    r.evt_block_number
  from src_MorphoBlue_evt_Repay r
    inner join markets m on r.id = m.id
  union all
  select
    'borrow_liquidation' as transaction_type,
    null as loan_type,
    m.loanToken as token_address,
    l.borrower,
    l.borrower as on_behalf_of,
    l.caller as repayer,
    l.caller as liquidator,
    -1 * cast(l.repaidAssets as double) as amount,
    l.contract_address,
    l.evt_tx_hash,
    l.evt_index,
    l.evt_block_time,
    l.evt_block_number
  from src_MorphoBlue_evt_Liquidate l
    inner join markets m on l.id = m.id
)

select
  '{{ blockchain }}' as blockchain,
  '{{ project }}' as project,
  '{{ version }}' as version,
  transaction_type,
  loan_type,
  token_address,
  borrower,
  on_behalf_of,
  repayer,
  liquidator,
  amount,
  cast(date_trunc('month', evt_block_time) as date) as block_month,
  evt_block_time as block_time,
  evt_block_number as block_number,
  contract_address as project_contract_address,
  evt_tx_hash as tx_hash,
  evt_index
from base_borrow

{% endmacro %}
