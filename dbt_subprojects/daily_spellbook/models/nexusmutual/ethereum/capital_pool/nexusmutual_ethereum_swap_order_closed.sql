{{
  config(
    tags = ['prod_exclude'],
    schema = 'nexusmutual_ethereum',
    alias = 'swap_order_closed',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'fill_type', 'tx_hash'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    post_hook = '{{ expose_spells(blockchains = \'["ethereum"]\',
                                  spell_type = "project",
                                  spell_name = "nexusmutual",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

with

-- old swaps
swapped_raw as (
  select
    evt_block_time as block_time,
    evt_block_number as block_number,
    contract_address as swap_operator_contract,
    fromAsset as sell_token,
    toAsset as buy_token,
    amountIn as sell_amount_raw,
    amountOut as buy_amount_raw,
    evt_tx_hash as tx_hash
  from (
      select * from {{ source('nexusmutual_ethereum', 'swapoperator_evt_swapped') }}
      {% if is_incremental() %}
      where {{ incremental_predicate('evt_block_time') }}
      {% endif %}
      union all
      select * from {{ source('nexusmutual_ethereum_ethereum', 'swapoperator_evt_swapped') }}
      {% if is_incremental() %}
      where {{ incremental_predicate('evt_block_time') }}
      {% endif %}
    ) s
),

-- new limit orders
order_closed_raw as (
  select
    evt_block_time as block_time,
    evt_block_number as block_number,
    filledAmount as filled_amount,
    from_hex(json_query("order", 'lax $.sellToken' omit quotes)) as sell_token,
    from_hex(json_query("order", 'lax $.buyToken' omit quotes)) as buy_token,
    from_hex(json_query("order", 'lax $.receiver' omit quotes)) as receiver,
    cast(json_query("order", 'lax $.sellAmount') as uint256) as sell_amount,
    cast(json_query("order", 'lax $.buyAmount') as uint256) as buy_amount,
    cast(json_query("order", 'lax $.feeAmount') as uint256) as fee_amount,
    cast(json_query("order", 'lax $.partiallyFillable') as boolean) as partially_fillable,
    --"order",
    evt_tx_hash as tx_hash
  from {{ source('nexusmutual_ethereum_ethereum', 'swapoperator_evt_orderclosed') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
),

swapped_ext as (
  select
    s.block_time,
    date_trunc('day', s.block_time) as block_date,
    s.block_number,
    s.swap_operator_contract,
    0xcafea35ce5a2fc4ced4464da4349f81a122fd12b as capital_pool_contract,
    case
      when s.sell_token = 0x27F23c710dD3d878FE9393d93465FeD1302f2EbD then 'NXMTY'
      when starts_with(st.symbol, 'W') then substr(st.symbol, 2)
      else st.symbol
    end as sell_token_symbol,
    case
      when s.buy_token = 0x27F23c710dD3d878FE9393d93465FeD1302f2EbD then 'NXMTY'
      when starts_with(bt.symbol, 'W') then substr(bt.symbol, 2)
      else bt.symbol
    end as buy_token_symbol,
    s.sell_amount_raw / power(10, coalesce(st.decimals, 18)) as sell_amount,
    s.buy_amount_raw / power(10, coalesce(bt.decimals, 18)) as buy_amount,
    s.sell_amount_raw,
    s.buy_amount_raw,
    s.tx_hash
  from swapped_raw s
    left join {{ source('tokens', 'erc20') }} st on s.sell_token = st.contract_address and st.blockchain = 'ethereum'
    left join {{ source('tokens', 'erc20') }} bt on s.buy_token = bt.contract_address and bt.blockchain = 'ethereum'  
),

order_closed_ext as (
  select
    o.block_time,
    date_trunc('day', o.block_time) as block_date,
    o.block_number,
    if(starts_with(st.symbol, 'W'), substr(st.symbol, 2), st.symbol) as sell_token_symbol,
    if(starts_with(bt.symbol, 'W'), substr(bt.symbol, 2), bt.symbol) as buy_token_symbol,
    o.receiver,
    o.sell_amount / power(10, st.decimals) as sell_amount,
    o.buy_amount / power(10, bt.decimals) as buy_amount,
    o.sell_amount as sell_amount_raw,
    o.buy_amount as buy_amount_raw,
    o.filled_amount as filled_amount_raw,
    o.fee_amount as fee_amount_raw,
    o.partially_fillable,
    o.tx_hash
  from order_closed_raw o
    inner join {{ source('tokens', 'erc20') }} st on o.sell_token = st.contract_address and st.blockchain = 'ethereum'
    inner join {{ source('tokens', 'erc20') }} bt on o.buy_token = bt.contract_address and bt.blockchain = 'ethereum'
)

select
  block_time,
  block_date,
  block_number,
  'full fill' as fill_type,
  swap_operator_contract,
  capital_pool_contract,
  sell_token_symbol,
  buy_token_symbol,
  sell_amount,
  buy_amount,
  buy_amount as fill_amount,
  false as partially_fillable,
  tx_hash
from swapped_ext

union all

select
  o.block_time,
  o.block_date,
  o.block_number,
  case
    when o.filled_amount_raw = 0 and o.sell_token_symbol = t.symbol and o.sell_amount_raw = t.amount_raw then 'no fill'
    when o.filled_amount_raw > 0 and o.buy_token_symbol = t.symbol and o.buy_amount_raw <= t.amount_raw then 'full fill'
    when o.filled_amount_raw > 0 and o.buy_token_symbol = t.symbol and o.buy_amount_raw > t.amount_raw then 'partial fill'
    when o.filled_amount_raw > 0 and o.sell_token_symbol = t.symbol then 'funds returned'
  end as fill_type,
  o.receiver as swap_operator_contract,
  t.to as capital_pool_contract,
  o.sell_token_symbol,
  o.buy_token_symbol,
  o.sell_amount,
  o.buy_amount,
  t.amount as fill_amount,
  o.partially_fillable,
  o.tx_hash
from order_closed_ext o
  inner join {{ source('tokens_ethereum', 'transfers') }} t
    on o.block_time = t.block_time
    and o.block_number = t.block_number
    and o.tx_hash = t.tx_hash
    and o.receiver = t."from"
where t.block_time >= timestamp '2023-07-21'
  and ((o.filled_amount_raw = 0 and o.sell_token_symbol = t.symbol and o.sell_amount_raw = t.amount_raw) -- no fill
    or (o.filled_amount_raw > 0 and o.buy_token_symbol = t.symbol) -- full/partial fill
    or (o.filled_amount_raw > 0 and o.sell_token_symbol = t.symbol)) -- funds returned (on partial fill)
