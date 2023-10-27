{%
  macro lending_aave_v1_fork_borrow(
    blockchain,
    project,
    version,
    aave_mock_address,
    native_token_address,
    project_decoded_as = 'aave'
  )
%}

with

src_LendingPool_evt_Borrow as (
  select * from {{ source(project_decoded_as ~ '_' ~ blockchain, 'LendingPool_evt_Borrow') }}
),

src_LendingPool_evt_Repay as (
  select * from {{ source(project_decoded_as ~ '_' ~ blockchain, 'LendingPool_evt_Repay') }}
),

src_LendingPool_evt_LiquidationCall as (
  select * from {{ source(project_decoded_as ~ '_' ~ blockchain, 'LendingPool_evt_LiquidationCall') }}
),

base_borrow as (
  select
    'borrow' as transaction_type,
    case 
      when _borrowRateMode = uint256 '1' then 'stable'
      when _borrowRateMode = uint256 '2' then 'variable'
    end as loan_type,
    case
      when _reserve = {{ aave_mock_address }} then {{ native_token_address }} --using native_token_address instead of Aave "mock" address
      else _reserve
    end as token_address,
    _user as borrower,
    cast(null as varbinary) as repayer,
    cast(null as varbinary) as liquidator,
    cast(_amount as double) as amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_LendingPool_evt_Borrow
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
  union all
  select
    'repay' as transaction_type,
    null as loan_type,
    case
      when _reserve = {{ aave_mock_address }} then {{ native_token_address }} --using native_token_address instead of Aave "mock" address
      else _reserve
    end as token_address,
    _user as borrower,
    _repayer as repayer,
    cast(null as varbinary) as liquidator,
    -1 * cast(_amountMinusFees as double) as amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_LendingPool_evt_Repay
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
  union all
  select
    'borrow_liquidation' as transaction_type,
    null as loan_type,
    case
      when _reserve = {{ aave_mock_address }} then {{ native_token_address }} --using native_token_address instead of Aave "mock" address
      else _reserve
    end as token_address,
    _user as borrower,
    _liquidator as repayer,
    _liquidator as liquidator,
    -1 * cast(_purchaseAmount as double) as amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_LendingPool_evt_LiquidationCall
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
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
  evt_block_time,
  evt_block_number
from base_borrow

{% endmacro %}
