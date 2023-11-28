{%
  macro lending_compound_v2_compatible_borrow(
    blockchain = '',
    project = '',
    version = '',
    sources = []
  )
%}

with

src_Borrow as (
  {% for src in sources %}
    select contract_address, borrower, borrowAmount, evt_tx_hash, evt_index, evt_block_time, evt_block_number
    from {{ source( src["decoded_project"] ~ '_' ~ blockchain, src["contract"] ~ '_evt_Borrow' )}}
    {% if is_incremental() %}
    where {{ incremental_predicate('evt_block_time') }}
    {% endif %}
    {% if not loop.last %}
    union all
  {% endif %}
),

src_Repay as (
  {% for src in sources %}
    select contract_address, borrower, payer, repayAmount, evt_tx_hash, evt_index, evt_block_time, evt_block_number
    from {{ source( src["decoded_project"] ~ '_' ~ blockchain, src["contract"] ~ '_evt_RepayBorrow' )}}
    {% if is_incremental() %}
    where {{ incremental_predicate('evt_block_time') }}
    {% endif %}
    {% if not loop.last %}
    union all
  {% endif %}
),

src_LiquidationCall as (
  {% for src in sources %}
    select contract_address, borrower, liquidator, repayAmount, evt_tx_hash, evt_index, evt_block_time, evt_block_number
    from {{ source( src["decoded_project"] ~ '_' ~ blockchain, src["contract"] ~ '_evt_LiquidateBorrow' )}}
    {% if is_incremental() %}
    where {{ incremental_predicate('evt_block_time') }}
    {% endif %}
    {% if not loop.last %}
    union all
  {% endif %}
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
  left join {{ ref('compound_v2_' ~ blockchain ~ '_ctokens') }} on base_borrow.ctoken_address = ctokens.ctoken_address

{% endmacro %}

{# ######################################################################### #}
