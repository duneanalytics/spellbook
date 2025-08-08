{%
  macro lending_morpho_v1_compatible_supply(
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

src_MorphoBlue_evt_Supply as (
  select *
  from {{ source(decoded_project ~ '_' ~ blockchain, 'MorphoBlue_evt_Supply') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
),

src_MorphoBlue_evt_Withdraw as (
  select *
  from {{ source(decoded_project ~ '_' ~ blockchain, 'MorphoBlue_evt_Withdraw') }}
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

base_supply as (
  select
    'deposit' as transaction_type,
    m.loanToken as token_address,
    s.caller as depositor,
    s.onBehalf as on_behalf_of,
    cast(null as varbinary) as withdrawn_to,
    cast(null as varbinary) as liquidator,
    cast(s.assets as double) as amount,
    s.contract_address,
    s.evt_tx_hash,
    s.evt_index,
    s.evt_block_time,
    s.evt_block_number
  from src_MorphoBlue_evt_Supply s
    inner join markets m on s.id = m.id
  union all
  select
    'withdraw' as transaction_type,
    m.loanToken as token_address,
    cast(null as varbinary) as depositor,
    w.onBehalf as on_behalf_of,
    w.receiver as withdrawn_to,
    cast(null as varbinary) as liquidator,
    -1 * cast(w.assets as double) as amount,
    w.contract_address,
    w.evt_tx_hash,
    w.evt_index,
    w.evt_block_time,
    w.evt_block_number
  from src_MorphoBlue_evt_Withdraw w
    inner join markets m on w.id = m.id
  union all
  select
    'deposit_liquidation' as transaction_type,
    m.loanToken as token_address,
    l.borrower as depositor,
    cast(null as varbinary) as on_behalf_of,
    l.caller as withdrawn_to,
    l.caller as liquidator,
    -1 * cast(l.seizedAssets as double) as amount,
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
  token_address,
  depositor,
  on_behalf_of,
  withdrawn_to,
  liquidator,
  amount,
  cast(date_trunc('month', evt_block_time) as date) as block_month,
  evt_block_time as block_time,
  evt_block_number as block_number,
  contract_address as project_contract_address,
  evt_tx_hash as tx_hash,
  evt_index
from base_supply

{% endmacro %}
