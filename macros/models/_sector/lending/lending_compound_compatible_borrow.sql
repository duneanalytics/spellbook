{%
  macro lending_compound_v2_compatible_borrow(
    blockchain = '',
    project = '',
    version = '',
    decoded_project = 'compound_v2',
    sources = []
  )
%}

with

src_Borrow as (
  {% for src in sources %}
    select
      contract_address,
      {% if src["borrower_column_name"] -%}
        {{ src["borrower_column_name"] }} as 
      {%- endif %}
      borrower,
      {% if src["borrowAmount_column_name"] -%}
        {{ src["borrowAmount_column_name"] }} as 
      {%- endif %}
      borrowAmount,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number
    from {{ source( decoded_project ~ '_' ~ blockchain, src["contract"] ~ '_evt_Borrow' )}}
    {% if is_incremental() %}
    where {{ incremental_predicate('evt_block_time') }}
    {% endif %}
    {% if not loop.last %}
    union all
    {% endif %}
  {% endfor %}
),

src_Repay as (
  {% for src in sources %}
    select
      contract_address,
      borrower,
      payer,
      {% if src["repayAmount_column_name"] -%}
        {{ src["repayAmount_column_name"] }} as 
      {%- endif %}
      repayAmount,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number
    from {{ source( decoded_project ~ '_' ~ blockchain, src["contract"] ~ '_evt_RepayBorrow' )}}
    {% if is_incremental() %}
    where {{ incremental_predicate('evt_block_time') }}
    {% endif %}
    {% if not loop.last %}
    union all
    {% endif %}
  {% endfor %}
),

src_LiquidationCall as (
  {% for src in sources %}
    select
      contract_address,
      borrower,
      liquidator,
      {% if src["repayAmount_column_name"] -%}
        {{ src["repayAmount_column_name"] }} as 
      {%- endif %}
      repayAmount,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number
    from {{ source( decoded_project ~ '_' ~ blockchain, src["contract"] ~ '_evt_LiquidateBorrow' )}}
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

base_borrow as (
  select
    'borrow' as transaction_type,
    contract_address as ctoken_address,
    borrower,
    cast(null as varbinary) as repayer,
    cast(null as varbinary) as liquidator,
    cast(borrowAmount as double) as amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_Borrow
  union all
  select
    'repay' as transaction_type,
    contract_address as ctoken_address,
    borrower,
    payer as repayer,
    cast(null as varbinary) as liquidator,
    -1 * cast(repayAmount as double) as amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_Repay
  union all
  select
    'borrow_liquidation' as transaction_type,
    contract_address as ctoken_address,
    borrower,
    liquidator as repayer,
    liquidator,
    -1 * cast(repayAmount as double) as amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_LiquidationCall
)

select
  '{{ blockchain }}' as blockchain,
  '{{ project }}' as project,
  '{{ version }}' as version,
  base_borrow.transaction_type,
  cast(null as varchar) as loan_type,
  ctokens.asset_address as token_address,
  base_borrow.borrower,
  base_borrow.repayer,
  base_borrow.liquidator,
  base_borrow.amount,
  cast(date_trunc('month', base_borrow.evt_block_time) as date) as block_month,
  base_borrow.evt_block_time as block_time,
  base_borrow.evt_block_number as block_number,
  base_borrow.evt_tx_hash as tx_hash,
  base_borrow.evt_index
from base_borrow
  join ctokens on base_borrow.ctoken_address = ctokens.ctoken_address

{% endmacro %}

{# ######################################################################### #}

{%
  macro lending_compound_v3_compatible_borrow(
    blockchain = '',
    project = '',
    version = '',
    decoded_project = 'compound_v3',
    sources = []
  )
%}

with

src_Borrow as (
  {% for src in sources %}
    select contract_address, src, amount, evt_tx_hash, evt_index, evt_block_time, evt_block_number
    from {{ source( decoded_project ~ '_' ~ blockchain, src["contract"] ~ '_evt_Withdraw' )}}
    where evt_tx_hash not in (
        select evt_tx_hash
        from {{ source( decoded_project ~ '_' ~ blockchain, src["contract"] ~ '_evt_Transfer' )}}
        where "to" = 0x0000000000000000000000000000000000000000
      )
    {% if is_incremental() %}
      and {{ incremental_predicate('evt_block_time') }}
    {% endif %}
    {% if not loop.last %}
    union all
    {% endif %}
  {% endfor %}
),

src_Repay as (
  {% for src in sources %}
    select contract_address, "from", dst, amount, evt_tx_hash, evt_index, evt_block_time, evt_block_number
    from {{ source( decoded_project ~ '_' ~ blockchain, src["contract"] ~ '_evt_Supply' )}}
    where evt_tx_hash not in (
        select evt_tx_hash
        from {{ source( decoded_project ~ '_' ~ blockchain, src["contract"] ~ '_evt_Transfer' )}}
        where "from" = 0x0000000000000000000000000000000000000000
      )
    {% if is_incremental() %}
      and {{ incremental_predicate('evt_block_time') }}
    {% endif %}
    {% if not loop.last %}
    union all
    {% endif %}
  {% endfor %}
),

src_LiquidationCall as (
  {% for src in sources %}
    select contract_address, borrower, absorber, basePaidOut, evt_tx_hash, evt_index, evt_block_time, evt_block_number
    from {{ source( decoded_project ~ '_' ~ blockchain, src["contract"] ~ '_evt_AbsorbDebt' )}}
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

base_borrow as (
  select
    'borrow' as transaction_type,
    contract_address as comet_contract_address,
    src as borrower,
    cast(null as varbinary) as repayer,
    cast(null as varbinary) as liquidator,
    cast(amount as double) as amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_Borrow
  union all
  select
    'repay' as transaction_type,
    contract_address as comet_contract_address,
    "from" as borrower,
    dst as repayer,
    cast(null as varbinary) as liquidator,
    -1 * cast(amount as double) as amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_Repay
  union all
  select
    'borrow_liquidation' as transaction_type,
    contract_address as comet_contract_address,
    borrower,
    absorber as repayer,
    absorber as liquidator,
    -1 * cast(basePaidOut as double) as amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_LiquidationCall
)

select
  '{{ blockchain }}' as blockchain,
  '{{ project }}' as project,
  '{{ version }}' as version,
  base_borrow.transaction_type,
  cast(null as varchar) as loan_type,
  ctokens.asset_address as token_address,
  base_borrow.borrower,
  base_borrow.repayer,
  base_borrow.liquidator,
  base_borrow.amount,
  cast(date_trunc('month', base_borrow.evt_block_time) as date) as block_month,
  base_borrow.evt_block_time as block_time,
  base_borrow.evt_block_number as block_number,
  base_borrow.evt_tx_hash as tx_hash,
  base_borrow.evt_index
from base_borrow
  join (
    select distinct comet_contract_address, asset_address from ctokens
  ) ctokens on base_borrow.comet_contract_address = ctokens.comet_contract_address

{% endmacro %}
