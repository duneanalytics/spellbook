{%
  macro lending_aave_v1_compatible_supply(
    blockchain,
    project,
    version,
    aave_mock_address,
    native_token_address,
    project_decoded_as = 'aave'
  )
%}

with

src_LendingPool_evt_Deposit as (
  select *
  from {{ source(project_decoded_as ~ '_' ~ blockchain, 'LendingPool_evt_Deposit') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
),

src_LendingPool_evt_Withdraw as (
  select *
  from {{ source(project_decoded_as ~ '_' ~ blockchain, 'LendingPool_evt_RedeemUnderlying') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
),

src_LendingPool_evt_LiquidationCall as (
  select *
  from {{ source(project_decoded_as ~ '_' ~ blockchain, 'LendingPool_evt_LiquidationCall') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
),

base_supply as (
  select
    'deposit' as transaction_type,
    case
      when _reserve = {{ aave_mock_address }} then {{ native_token_address }} --using native_token_address instead of Aave "mock" address
      else _reserve
    end as token_address,
    _user as depositor,
    cast(null as varbinary) as withdrawn_to,
    cast(null as varbinary) as liquidator,
    cast(_amount as double) as amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_LendingPool_evt_Deposit
  union all
  select
    'withdraw' as transaction_type,
    case
      when _reserve = {{ aave_mock_address }} then {{ native_token_address }} --using native_token_address instead of Aave "mock" address
      else _reserve
    end as token_address,
    _user as depositor,
    _user as withdrawn_to,
    cast(null as varbinary) as liquidator,
    -1 * cast(_amount as double) as amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_LendingPool_evt_Withdraw
  union all
  select
    'deposit_liquidation' as transaction_type,
    case
      when _collateral = {{ aave_mock_address }} then {{ native_token_address }} --using native_token_address instead of Aave "mock" address
      else _collateral
    end as token_address,
    _user as depositor,
    _liquidator as withdrawn_to,
    _liquidator as liquidator,
    -1 * cast(_liquidatedCollateralAmount as double) as amount,
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
  token_address,
  depositor,
  withdrawn_to,
  liquidator,
  amount,
  cast(date_trunc('month', evt_block_time) as date) as block_month,
  evt_block_time as block_time,
  evt_block_number as block_number,
  evt_tx_hash as tx_hash,
  evt_index
from base_supply

{% endmacro %}

{# ######################################################################### #}

{%
  macro lending_aave_v2_compatible_supply(
    blockchain,
    project,
    version,
    project_decoded_as = 'aave_v2'
  )
%}

with 

src_LendingPool_evt_Deposit as (
  select *
  from {{ source(project_decoded_as ~ '_' ~ blockchain, 'LendingPool_evt_Deposit') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
),

src_LendingPool_evt_Withdraw as (
  select *
  from {{ source(project_decoded_as ~ '_' ~ blockchain, 'LendingPool_evt_Withdraw') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
),

src_LendingPool_evt_LiquidationCall as (
  select *
  from {{ source(project_decoded_as ~ '_' ~ blockchain, 'LendingPool_evt_LiquidationCall') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
),

base_supply as (
  select
    'deposit' as transaction_type,
    reserve as token_address,
    user as depositor,
    cast(null as varbinary) as withdrawn_to,
    cast(null as varbinary) as liquidator,
    cast(amount as double) as amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_LendingPool_evt_Deposit
  union all
  select
    'withdraw' as transaction_type,
    reserve as token_address,
    user as depositor,
    to as withdrawn_to,
    cast(null as varbinary) as liquidator,
    -1 * cast(amount as double) as amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_LendingPool_evt_Withdraw
  union all
  select
    'deposit_liquidation' as transaction_type,
    collateralAsset as token_address,
    user as depositor,
    liquidator as withdrawn_to,
    liquidator as liquidator,
    -1 * cast(liquidatedCollateralAmount as double) as amount,
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
  token_address,
  depositor,
  withdrawn_to,
  liquidator,
  amount,
  cast(date_trunc('month', evt_block_time) as date) as block_month,
  evt_block_time as block_time,
  evt_block_number as block_number,
  evt_tx_hash as tx_hash,
  evt_index
from base_supply

{% endmacro %}

{# ######################################################################### #}

{%
  macro lending_aave_v3_compatible_supply(
    blockchain,
    project,
    version,
    project_decoded_as = 'aave_v3',
    decoded_contract_name = 'Pool'
  )
%}

with 

src_LendingPool_evt_Deposit as (
  select *
  from {{ source(project_decoded_as ~ '_' ~ blockchain, decoded_contract_name ~ '_evt_Supply') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
),

src_LendingPool_evt_Withdraw as (
  select *
  from {{ source(project_decoded_as ~ '_' ~ blockchain, decoded_contract_name ~ '_evt_Withdraw') }}
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

base_supply as (
  select
    'deposit' as transaction_type,
    reserve as token_address,
    user as depositor,
    cast(null as varbinary) as withdrawn_to,
    cast(null as varbinary) as liquidator,
    cast(amount as double) as amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_LendingPool_evt_Deposit
  union all
  select
    'withdraw' as transaction_type,
    reserve as token_address,
    user as depositor,
    to as withdrawn_to,
    cast(null as varbinary) as liquidator,
    -1 * cast(amount as double) as amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_LendingPool_evt_Withdraw
  union all
  select
    'deposit_liquidation' as transaction_type,
    collateralAsset as token_address,
    user as depositor,
    liquidator as withdrawn_to,
    liquidator as liquidator,
    -1 * cast(liquidatedCollateralAmount as double) as amount,
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
  token_address,
  depositor,
  withdrawn_to,
  liquidator,
  amount,
  cast(date_trunc('month', evt_block_time) as date) as block_month,
  evt_block_time as block_time,
  evt_block_number as block_number,
  evt_tx_hash as tx_hash,
  evt_index
from base_supply

{% endmacro %}
