{{
  config(
    tags = ['prod_exclude'],
    schema = 'nexusmutual_ethereum',
    alias = 'swap_order_placed',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'order_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    post_hook = '{{ expose_spells(blockchains = \'["ethereum"]\',
                                  spell_type = "project",
                                  spell_name = "nexusmutual",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

with

order_placed as (
  select
    eo.evt_block_time as block_time,
    eo.evt_block_number as block_number,
    co.orderUID as order_id,
    from_hex(json_query(eo."order", 'lax $.sellToken' omit quotes)) as sell_token,
    from_hex(json_query(eo."order", 'lax $.buyToken' omit quotes)) as buy_token,
    from_hex(json_query(eo."order", 'lax $.receiver' omit quotes)) as receiver,
    cast(json_query(eo."order", 'lax $.sellAmount') as uint256) as sell_amount,
    cast(json_query(eo."order", 'lax $.buyAmount') as uint256) as buy_amount,
    cast(json_query(eo."order", 'lax $.feeAmount') as uint256) as fee_amount,
    cast(json_query(eo."order", 'lax $.partiallyFillable') as boolean) as partially_fillable,
    --eo."order",
    eo.evt_tx_hash as tx_hash
  from {{ source('nexusmutual_ethereum_ethereum', 'swapoperator_evt_orderplaced') }} eo
    inner join {{ source('nexusmutual_ethereum_ethereum', 'swapoperator_call_placeorder') }} co on eo.evt_block_time = co.call_block_time and eo.evt_tx_hash = co.call_tx_hash
  where co.call_success
    {% if is_incremental() %}
    and {{ incremental_predicate('eo.evt_block_time') }}
    {% endif %}
),

order_placed_ext as (
  select
    o.block_time,
    date_trunc('day', o.block_time) as block_date,
    o.block_number,
    o.order_id,
    if(starts_with(st.symbol, 'W'), substr(st.symbol, 2), st.symbol) as sell_token_symbol,
    if(starts_with(bt.symbol, 'W'), substr(bt.symbol, 2), bt.symbol) as buy_token_symbol,
    o.receiver,
    o.sell_amount / power(10, st.decimals) as sell_amount,
    o.buy_amount / power(10, bt.decimals) as buy_amount,
    o.sell_amount as sell_amount_raw,
    o.buy_amount as buy_amount_raw,
    o.fee_amount as fee_amount_raw,
    o.partially_fillable,
    o.tx_hash
  from order_placed o
    inner join {{ source('tokens', 'erc20') }} st on o.sell_token = st.contract_address and st.blockchain = 'ethereum'
    inner join {{ source('tokens', 'erc20') }} bt on o.buy_token = bt.contract_address and bt.blockchain = 'ethereum'
)

select
  o.block_time,
  o.block_date,
  o.block_number,
  t."from" as capital_pool_contract,
  o.receiver as swap_operator_contract,
  o.sell_token_symbol,
  o.buy_token_symbol,
  o.sell_amount,
  o.buy_amount,
  o.fee_amount_raw,
  o.partially_fillable,
  o.order_id,
  o.tx_hash
from order_placed_ext o
  inner join {{ source('tokens_ethereum', 'transfers') }} t
    on o.block_time = t.block_time
    and o.block_number = t.block_number
    and o.tx_hash = t.tx_hash
    and o.receiver = t.to
    and o.sell_token_symbol = t.symbol
    --and o.sell_amount_raw = t.amount_raw
where t.block_time >= timestamp '2023-07-21'
