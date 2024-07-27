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
    cast(null as varbinary) as on_behalf_of,
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
    cast(null as varbinary) as on_behalf_of,
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
  base_supply.on_behalf_of,
  base_supply.withdrawn_to,
  base_supply.liquidator,
  base_supply.amount,
  cast(date_trunc('month', base_supply.evt_block_time) as date) as block_month,
  base_supply.evt_block_time as block_time,
  base_supply.evt_block_number as block_number,
  base_supply.ctoken_address as project_contract_address,
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

base_supply as (
  select
    'supply' as transaction_type,
    asset as token_address,
    "from" as depositor,
    cast(null as varbinary) as on_behalf_of,
    cast(null as varbinary) as withdrawn_to,
    cast(null as varbinary) as liquidator,
    cast(amount as double) as amount,
    contract_address,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_SupplyCollateral
  union all
  select
    'withdraw' as transaction_type,
    asset as token_address,
    src as depositor,
    cast(null as varbinary) as on_behalf_of,
    to as withdrawn_to,
    cast(null as varbinary) as liquidator,
    -1 * cast(amount as double) as amount,
    contract_address,
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
    cast(null as varbinary) as on_behalf_of,
    absorber as withdrawn_to,
    absorber as liquidator,
    -1 * cast(collateralAbsorbed as double) as amount,
    contract_address,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from src_AbsorbCollateral

  union all
  select
    'supply' as transaction_type,
    ctokens.asset_address as token_address,
    supply_from as depositor,
    supply_dst as on_behalf_of,
    cast(null as varbinary) as withdrawn_to,
    cast(null as varbinary) as liquidator,
    cast(amount_supplied as double) as amount,
    contract_address,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from base_supply_actions
    join (
      select distinct comet_contract_address, asset_address from ctokens
    ) ctokens on base_supply_actions.contract_address = ctokens.comet_contract_address
  where amount_supplied is not null 
    and amount_supplied > 0

  union all
  select
    'withdraw' as transaction_type,
    ctokens.asset_address as token_address,
    withdraw_from as depositor,
    cast(null as varbinary) as on_behalf_of,
    withdraw_dst as withdrawn_to,
    cast(null as varbinary) as liquidator,
    -1 * cast(amount_withdrawn as double) as amount,
    contract_address,
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number
  from base_withdraw_actions
    join (
      select distinct comet_contract_address, asset_address from ctokens
    ) ctokens on base_withdraw_actions.contract_address = ctokens.comet_contract_address
  where amount_withdrawn is not null 
    and amount_withdrawn > 0
)

select
  '{{ blockchain }}' as blockchain,
  '{{ project }}' as project,
  '{{ version }}' as version,
  base_supply.transaction_type,
  cast(null as varchar) as loan_type,
  base_supply.token_address,
  base_supply.depositor,
  base_supply.on_behalf_of,
  base_supply.withdrawn_to,
  base_supply.liquidator,
  base_supply.amount,
  cast(date_trunc('month', base_supply.evt_block_time) as date) as block_month,
  base_supply.evt_block_time as block_time,
  base_supply.evt_block_number as block_number,
  base_supply.contract_address as project_contract_address,
  base_supply.evt_tx_hash as tx_hash,
  base_supply.evt_index
from base_supply

{% endmacro %}
