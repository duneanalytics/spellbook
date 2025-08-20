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
    cast(null as varbinary) as on_behalf_of,
    cast(null as varbinary) as withdrawn_to,
    cast(null as varbinary) as liquidator,
    cast(_amount as double) as amount,
    contract_address,
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
    cast(null as varbinary) as on_behalf_of,
    _user as withdrawn_to,
    cast(null as varbinary) as liquidator,
    -1 * cast(_amount as double) as amount,
    contract_address,
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
    cast(null as varbinary) as on_behalf_of,
    _liquidator as withdrawn_to,
    _liquidator as liquidator,
    -1 * cast(_liquidatedCollateralAmount as double) as amount,
    contract_address,
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

{# ######################################################################### #}

{%
  macro lending_aave_v2_compatible_supply(
    blockchain,
    project,
    version,
    project_decoded_as = 'aave_v2',
    wrapped_token_gateway_available = true
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

{% if wrapped_token_gateway_available %}
src_WrappedTokenGatewayV2_call_withdrawETH as (
  select *
  from {{ source(project_decoded_as ~ '_' ~ blockchain, 'WrappedTokenGatewayV2_call_withdrawETH') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('call_block_time') }}
  {% endif %}
),
{% endif %}

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
    onBehalfOf as on_behalf_of,
    cast(null as varbinary) as withdrawn_to,
    cast(null as varbinary) as liquidator,
    cast(amount as double) as amount,
    contract_address,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_LendingPool_evt_Deposit
  union all
  select
    'withdraw' as transaction_type,
    w.reserve as token_address,
    w.user as depositor,
    {% if wrapped_token_gateway_available %}
      cast(wrap.to as varbinary)
    {% else %}
      cast(null as varbinary)
    {% endif %} as on_behalf_of,
    w.to as withdrawn_to,
    cast(null as varbinary) as liquidator,
    -1 * cast(w.amount as double) as amount,
    w.contract_address,
    w.evt_tx_hash,
    w.evt_index,
    w.evt_block_time,
    w.evt_block_number
  from src_LendingPool_evt_Withdraw w
  {% if wrapped_token_gateway_available %}
    left join src_WrappedTokenGatewayV2_call_withdrawETH wrap
      on w.evt_block_number = wrap.call_block_number
      and w.evt_tx_hash = wrap.call_tx_hash
      and w.to = wrap.contract_address
      and w.amount = wrap.amount
      and wrap.call_success
  {% endif %}
  union all
  select
    'deposit_liquidation' as transaction_type,
    collateralAsset as token_address,
    user as depositor,
    cast(null as varbinary) as on_behalf_of,
    liquidator as withdrawn_to,
    liquidator as liquidator,
    -1 * cast(liquidatedCollateralAmount as double) as amount,
    contract_address,
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

{# ######################################################################### #}

{%
  macro lending_aave_v3_compatible_supply(
    blockchain,
    project,
    version,
    project_decoded_as = 'aave_v3',
    decoded_contract_name = 'Pool',
    wrapped_token_gateway_available = true,
    decoded_wrapped_token_gateway_name = 'WrappedTokenGatewayV3'
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

{% if wrapped_token_gateway_available %}
src_WrappedTokenGatewayV3_call_withdrawETH as (
  select *
  from {{ source(project_decoded_as ~ '_' ~ blockchain, decoded_wrapped_token_gateway_name ~ '_call_withdrawETH') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('call_block_time') }}
  {% endif %}
),
{% endif %}

src_LendingPool_evt_Repay as (
  select *
  from {{ source(project_decoded_as ~ '_' ~ blockchain, decoded_contract_name ~ '_evt_Repay') }}
  where useATokens -- ref: https://github.com/duneanalytics/spellbook/issues/6417
  {% if is_incremental() %}
  and {{ incremental_predicate('evt_block_time') }}
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
    onBehalfOf as on_behalf_of,
    cast(null as varbinary) as withdrawn_to,
    cast(null as varbinary) as liquidator,
    cast(amount as double) as amount,
    contract_address,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_LendingPool_evt_Deposit
  union all
  select
    'withdraw' as transaction_type,
    w.reserve as token_address,
    w.user as depositor,
    {% if wrapped_token_gateway_available %}
      cast(wrap.to as varbinary)
    {% else %}
      cast(null as varbinary)
    {% endif %} as on_behalf_of,
    w.to as withdrawn_to,
    cast(null as varbinary) as liquidator,
    -1 * cast(w.amount as double) as amount,
    w.contract_address,
    w.evt_tx_hash,
    w.evt_index,
    w.evt_block_time,
    w.evt_block_number
  from src_LendingPool_evt_Withdraw w
  {% if wrapped_token_gateway_available %}
    left join src_WrappedTokenGatewayV3_call_withdrawETH wrap
      on w.evt_block_number = wrap.call_block_number
      and w.evt_tx_hash = wrap.call_tx_hash
      and w.to = wrap.contract_address
      and w.amount = wrap.amount
      and wrap.call_success
  {% endif %}
  union all
  select
    'repay_with_atokens' as transaction_type,
    reserve as token_address,
    user as depositor,
    cast(null as varbinary) as on_behalf_of,
    repayer as withdrawn_to,
    cast(null as varbinary) as liquidator,
    -1 * cast(amount as double) as amount,
    contract_address,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_LendingPool_evt_Repay
  union all
  select
    'deposit_liquidation' as transaction_type,
    collateralAsset as token_address,
    user as depositor,
    cast(null as varbinary) as on_behalf_of,
    liquidator as withdrawn_to,
    liquidator as liquidator,
    -1 * cast(liquidatedCollateralAmount as double) as amount,
    contract_address,
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

{# ######################################################################### #}

{%
  macro lending_aave_v3_compatible_supply_scaled(
    blockchain,
    project,
    version
  )
%}

with

hourly_market as (
  select
    blockchain,
    project,
    version,
    block_hour,
    token_address,
    symbol,
    liquidity_index,
    variable_borrow_index
  from {{ ref(project ~ '_v' ~ version ~ '_' ~ blockchain ~ '_base_market_hourly_agg') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('block_hour') }}
  {% endif %}
),

market_ordered as (
  select
    blockchain,
    project,
    version,
    block_time,
    block_hour,
    block_number,
    token_address,
    symbol,
    liquidity_index,
    variable_borrow_index,
    tx_hash,
    row_number() over (partition by block_time, tx_hash, token_address order by evt_index) as event_order
  from {{ ref(project ~ '_v' ~ version ~ '_' ~ blockchain ~ '_base_market') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('block_time') }}
  {% endif %}
),

supply_ordered as (
  select
    blockchain,
    project,
    version,
    block_time,
    block_number,
    coalesce(on_behalf_of, depositor) as user,
    token_address,
    symbol,
    amount,
    tx_hash,
    row_number() over (partition by block_time, tx_hash, token_address order by evt_index) as event_order
  from {{ ref('aave_' ~ blockchain ~ '_supply')}}
  where version = '3'
  {% if is_incremental() %}
  and {{ incremental_predicate('block_time') }}
  {% endif %}
),

supplied as (
  select
    s.blockchain,
    s.project,
    s.version,
    m.block_hour,
    s.user,
    s.token_address,
    s.symbol,
    sum(s.amount / m.liquidity_index * power(10, 27)) as atoken_amount
  from supply_ordered s
    inner join market_ordered m
      on s.block_number = m.block_number
      and s.block_time = m.block_time
      and s.event_order = m.event_order
      and s.tx_hash = m.tx_hash
      and s.token_address = m.token_address
  group by 1, 2, 3, 4, 5, 6, 7
),

first_supplied as (
  select
    blockchain,
    project,
    version,
    token_address,
    user,
    min(block_hour) as first_block_hour
  from supplied
  group by 1, 2, 3, 4, 5
),

hourly_market_user as (
  select
    hm.blockchain,
    hm.project,
    hm.version,
    hm.block_hour,
    hm.token_address,
    hm.symbol,
    hm.liquidity_index,
    hm.variable_borrow_index,
    fs.user
  from hourly_market hm
    inner join first_supplied fs
      on hm.block_hour >= fs.first_block_hour
      and hm.token_address = fs.token_address
)

select
  hmu.blockchain,
  hmu.project,
  hmu.version,
  cast(date_trunc('month', hmu.block_hour) as date) as block_month,
  hmu.block_hour,
  hmu.token_address,
  hmu.symbol,
  hmu.user,
  sum(s.atoken_amount) over (
    partition by hmu.token_address, hmu.user 
    order by hmu.block_hour
  ) * hmu.liquidity_index / power(10, 27) as amount
from hourly_market_user hmu
  left join supplied s
    on hmu.block_hour = s.block_hour
    and hmu.token_address = s.token_address
    and hmu.user = s.user

{% endmacro %}
