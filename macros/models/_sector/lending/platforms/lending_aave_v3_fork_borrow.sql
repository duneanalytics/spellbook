{%
  macro lending_aave_v3_fork_borrow(
    blockchain,
    project,
    version,
    project_decoded_as = 'aave_v3'
  )
%}

{%- set decoded_contract_name = 'L2Pool' if blockchain == 'arbitrum' else 'Pool' -%}

with 

src_LendingPool_evt_Borrow as (
  select *
  from {{ source(project_decoded_as ~ '_' ~ blockchain, decoded_contract_name ~ '_evt_Borrow') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
),

src_LendingPool_evt_Repay as (
  select *
  from {{ source(project_decoded_as ~ '_' ~ blockchain, decoded_contract_name ~ '_evt_Repay') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
),

src_LendingPool_evt_LiquidationCall as (
  select *
  from {{ source(project_decoded_as ~ '_' ~ blockchain, decoded_contract_name ~ '_evt_LiquidationCall') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
),

base_borrow as (
  select
    'borrow' as transaction_type,
    case 
      when interestRateMode = 1 then 'stable'
      when interestRateMode = 2 then 'variable'
    end as loan_type,
    reserve as token_address,
    user as borrower,
    cast(null as varbinary) as repayer,
    cast(null as varbinary) as liquidator,
    cast(amount as double) as amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_LendingPool_evt_Borrow
  union all
  select
    'repay' as transaction_type,
    null as loan_type,
    reserve as token_address,
    user as borrower,
    repayer as repayer,
    cast(null as varbinary) as liquidator,
    -1 * cast(amount as double) as amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_LendingPool_evt_Repay
  union all
  select
    'borrow_liquidation' as transaction_type,
    null as loan_type,
    debtAsset as token_address,
    user as borrower,
    liquidator as repayer,
    liquidator as liquidator,
    -1 * cast(debtToCover as double) as amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_LendingPool_evt_LiquidationCall
)

select
  '{{ blockchain }}' as blockchain,
  '{{ project }}' as project,
  '{{ version }}' as version,
  transaction_type,
  loan_type,
  token_address,
  borrower,
  repayer,
  liquidator,
  amount,
  evt_tx_hash,
  evt_index,
  cast(date_trunc('month', evt_block_time) as date) as evt_block_month,
  evt_block_time,
  evt_block_number
from base_borrow

{% endmacro %}
