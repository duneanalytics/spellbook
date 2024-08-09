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
    cast(null as varbinary) as on_behalf_of,
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
    cast(null as varbinary) as on_behalf_of,
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
    cast(null as varbinary) as on_behalf_of,
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
  base_borrow.on_behalf_of,
  base_borrow.repayer,
  base_borrow.liquidator,
  base_borrow.amount,
  cast(date_trunc('month', base_borrow.evt_block_time) as date) as block_month,
  base_borrow.evt_block_time as block_time,
  base_borrow.evt_block_number as block_number,
  base_borrow.ctoken_address as project_contract_address,
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
base_withdraw_actions as (
  {% for src in sources %}
    select 
      w.contract_address, w.evt_tx_hash, w.evt_index, w.evt_block_time, w.evt_block_number,
      
      w.src as withdraw_from,
      w.to as withdraw_dst,
      w.amount as withdraw_amt,
      
      t."from" as transfer_from,
      t.to as transfer_to,
      t.amount as transfer_amt,

      coalesce(least(t.amount,w.amount),0) as amount_withdrawn,
      w.amount - coalesce(least(t.amount,w.amount),0) as amount_borrowed,
      case
          when t.amount is null then 'borrow'
          when w.amount = t.amount then 'withdraw'
          when w.amount < t.amount+10 then 'withdraw' -- accuracy fix
          else 'borrow + withdraw' 
      end as action
    from {{ source( decoded_project ~ '_' ~ blockchain, src["contract"] ~ '_evt_Withdraw' )}} w 
        left join {{ source( decoded_project ~ '_' ~ blockchain, src["contract"] ~ '_evt_Transfer' )}} t 
            on w.evt_tx_hash = t.evt_tx_hash
            and w.contract_address = t.contract_address
            and w.evt_index+1 = t.evt_index
    {% if is_incremental() %}
    where {{ incremental_predicate('w.evt_block_time') }}
    {% endif %}
    {% if not loop.last %}
    union all
    {% endif %}
  {% endfor %}
),

base_supply_actions as (
  {% for src in sources %}
    select 
        s.contract_address, s.evt_tx_hash, s.evt_index, s.evt_block_time, s.evt_block_number,
        
        s."from" as supply_from,
        s.dst as supply_dst,
        s.amount as supply_amt,
        
        t."from" as transfer_from,
        t.to as transfer_to,
        t.amount as transfer_amt,

        coalesce(t.amount,0) as amount_supplied,
        s.amount - coalesce(t.amount,0) as amount_repaid,
        case
            when t.amount is null then 'repay'
            when s.amount = t.amount then 'supply'
            when s.amount < t.amount+10 then 'supply' -- accuracy fix
            else 'repay + supply' 
        end as action
    from {{ source( decoded_project ~ '_' ~ blockchain, src["contract"] ~ '_evt_Supply' )}} s
        left join {{ source( decoded_project ~ '_' ~ blockchain, src["contract"] ~ '_evt_Transfer' )}} t 
            on s.evt_tx_hash = t.evt_tx_hash
            and s.contract_address = t.contract_address
            and s.evt_index+1 = t.evt_index
    {% if is_incremental() %}
    where {{ incremental_predicate('s.evt_block_time') }}
    {% endif %}
    {% if not loop.last %}
    union all
    {% endif %}
  {% endfor %}
),

base_borrow as (
  select
    'borrow' as transaction_type,
    contract_address as comet_contract_address,
    withdraw_from as borrower,
    cast(null as varbinary) as on_behalf_of,
    cast(null as varbinary) as repayer,
    cast(null as varbinary) as liquidator,
    cast(amount_borrowed as double) as amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from base_withdraw_actions
  where amount_borrowed is not null
    and amount_borrowed> 0
  union all
  select
    'repay' as transaction_type,
    contract_address as comet_contract_address,
    supply_from as borrower,
    cast(null as varbinary) as on_behalf_of,
    supply_dst as repayer,
    cast(null as varbinary) as liquidator,
    -1 * cast(amount_repaid as double) as amount,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from base_supply_actions
  where amount_repaid is not null
    and amount_repaid> 0
  union all
  select
    'borrow_liquidation' as transaction_type,
    contract_address as comet_contract_address,
    borrower,
    cast(null as varbinary) as on_behalf_of,
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
  base_borrow.on_behalf_of,
  base_borrow.repayer,
  base_borrow.liquidator,
  base_borrow.amount,
  cast(date_trunc('month', base_borrow.evt_block_time) as date) as block_month,
  base_borrow.evt_block_time as block_time,
  base_borrow.evt_block_number as block_number,
  base_borrow.comet_contract_address as project_contract_address,
  base_borrow.evt_tx_hash as tx_hash,
  base_borrow.evt_index
from base_borrow
  join (
    select distinct comet_contract_address, asset_address from ctokens
  ) ctokens on base_borrow.comet_contract_address = ctokens.comet_contract_address

{% endmacro %}
