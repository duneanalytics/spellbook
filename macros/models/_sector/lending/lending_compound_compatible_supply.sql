{%
  macro lending_compound_v2_compatible_supply(
    blockchain = '',
    project = '',
    version = '',
    decoded_project = 'compound_v2',
    sources = []
  )
%}

with

src_Mint as (
  {% for src in sources %}
    select
      contract_address,
      minter,
      mintAmount,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number
    from {{ source( decoded_project ~ '_' ~ blockchain, src["contract"] ~ '_evt_Mint' )}}
    {% if is_incremental() %}
    where {{ incremental_predicate('evt_block_time') }}
    {% endif %}
    {% if not loop.last %}
    union all
    {% endif %}
  {% endfor %}
),

src_Redeem as (
  {% for src in sources %}
    select
      contract_address,
      {% if src["redeemer_column_name"] -%}
        {{ src["redeemer_column_name"] }} as 
      {%- endif %}
      redeemer,
      {% if src["redeemAmount_column_name"] -%}
        {{ src["redeemAmount_column_name"] }} as 
      {%- endif %}
      redeemAmount,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number
    from {{ source( decoded_project ~ '_' ~ blockchain, src["contract"] ~ '_evt_Redeem' )}}
    {% if is_incremental() %}
    where {{ incremental_predicate('evt_block_time') }}
    {% endif %}
    {% if not loop.last %}
    union all
    {% endif %}
  {% endfor %}
),

ctokens as (
  select * from {{ ref( decoded_project ~ '_' ~ blockchain ~ '_ctokens' ) }}
),

base_supply as (
  select
    'deposit' as transaction_type,
    contract_address as ctoken_address,
    minter as depositor,
    cast(null as varbinary) as withdrawn_to,
    cast(null as varbinary) as liquidator,
    cast(mintAmount as double) as amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_Mint
  union all
  select
    'withdraw' as transaction_type,
    contract_address as ctoken_address,
    redeemer as depositor,
    redeemer as withdrawn_to,
    cast(null as varbinary) as liquidator,
    -1 * cast(redeemAmount as double) as amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_Redeem
)

select
  '{{ blockchain }}' as blockchain,
  '{{ project }}' as project,
  '{{ version }}' as version,
  base_supply.transaction_type,
  cast(null as varchar) as loan_type,
  ctokens.asset_address as token_address,
  base_supply.depositor,
  base_supply.withdrawn_to,
  base_supply.liquidator,
  base_supply.amount,
  cast(date_trunc('month', base_supply.evt_block_time) as date) as block_month,
  base_supply.evt_block_time as block_time,
  base_supply.evt_block_number as block_number,
  base_supply.evt_tx_hash as tx_hash,
  base_supply.evt_index
from base_supply
  join ctokens on base_supply.ctoken_address = ctokens.ctoken_address

{% endmacro %}

{# ######################################################################### #}

{%
  macro lending_compound_v3_compatible_supply(
    blockchain = '',
    project = '',
    version = '',
    decoded_project = 'compound_v3',
    sources = []
  )
%}

with

src_SupplyCollateral as (
  {% for src in sources %}
    select contract_address, "from", amount, asset, evt_tx_hash, evt_index, evt_block_time, evt_block_number
    from {{ source( decoded_project ~ '_' ~ blockchain, src["contract"] ~ '_evt_SupplyCollateral' )}}
    {% if is_incremental() %}
    where {{ incremental_predicate('evt_block_time') }}
    {% endif %}
    {% if not loop.last %}
    union all
    {% endif %}
  {% endfor %}
),

src_WithdrawCollateral as (
  {% for src in sources %}
    select contract_address, src, to, amount, asset, evt_tx_hash, evt_index, evt_block_time, evt_block_number
    from {{ source( decoded_project ~ '_' ~ blockchain, src["contract"] ~ '_evt_WithdrawCollateral' )}}
    {% if is_incremental() %}
    where {{ incremental_predicate('evt_block_time') }}
    {% endif %}
    {% if not loop.last %}
    union all
    {% endif %}
  {% endfor %}
),

src_AbsorbCollateral as (
  {% for src in sources %}
    select contract_address, borrower, absorber, collateralAbsorbed, asset, evt_tx_hash, evt_index, evt_block_time, evt_block_number
    from {{ source( decoded_project ~ '_' ~ blockchain, src["contract"] ~ '_evt_AbsorbCollateral' )}}
    {% if is_incremental() %}
    where {{ incremental_predicate('evt_block_time') }}
    {% endif %}
    {% if not loop.last %}
    union all
    {% endif %}
  {% endfor %}
),

base_supply as (
  select
    'supply' as transaction_type,
    asset as token_address,
    "from" as depositor,
    cast(null as varbinary) as withdrawn_to,
    cast(null as varbinary) as liquidator,
    cast(amount as double) as amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_SupplyCollateral
  union all
  select
    'repay' as transaction_type,
    asset as token_address,
    src as depositor,
    to as withdrawn_to,
    cast(null as varbinary) as liquidator,
    -1 * cast(amount as double) as amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_WithdrawCollateral
  union all
  select
    'supply_liquidation' as transaction_type,
    asset as token_address,
    borrower as depositor,
    absorber as withdrawn_to,
    absorber as liquidator,
    -1 * cast(collateralAbsorbed as double) as amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_AbsorbCollateral
)

select
  '{{ blockchain }}' as blockchain,
  '{{ project }}' as project,
  '{{ version }}' as version,
  base_supply.transaction_type,
  cast(null as varchar) as loan_type,
  base_supply.token_address,
  base_supply.depositor,
  base_supply.withdrawn_to,
  base_supply.liquidator,
  base_supply.amount,
  cast(date_trunc('month', base_supply.evt_block_time) as date) as block_month,
  base_supply.evt_block_time as block_time,
  base_supply.evt_block_number as block_number,
  base_supply.evt_tx_hash as tx_hash,
  base_supply.evt_index
from base_supply

{% endmacro %}
