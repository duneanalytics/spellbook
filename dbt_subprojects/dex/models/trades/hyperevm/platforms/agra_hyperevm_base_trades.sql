{{ config(
    schema = 'agra_hyperevm'
    , alias = 'base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set agra_start_date = '2026-03-01' %}

with orders as (
    select
        evt_block_time     as block_time
        , evt_block_number as block_number
        , evt_tx_hash      as tx_hash
        , evt_index
        , contract_address as project_contract_address
        , offerer
        , recipient
        , orderHash        as order_hash
        , offer
        , consideration
    from {{ source('agra_multichain', 'settlement_evt_orderfulfilled') }}
    where chain = 'hyperevm'
    {% if is_incremental() %}
    and {{ incremental_predicate('evt_block_time') }}
    {% else %}
    and evt_block_time >= timestamp '{{ agra_start_date }}'
    {% endif %}
)

-- Order hashes settled via matchOrders. Such orders emit one OrderFulfilled per side, so the
-- taker's order leg (offerer = recipient) is the aggregate of the maker legs and is a duplicate.
, matched_hashes as (
    select evt_tx_hash as tx_hash, oh as order_hash
    from {{ source('agra_multichain', 'settlement_evt_ordersmatched') }}
    cross join unnest(orderHashes) as t(oh)
    where chain = 'hyperevm'
    {% if is_incremental() %}
    and {{ incremental_predicate('evt_block_time') }}
    {% else %}
    and evt_block_time >= timestamp '{{ agra_start_date }}'
    {% endif %}
    group by 1, 2
)

-- token the taker RECEIVES (offerer gives) -> token_bought
, offer_side as (
    select
        tx_hash
        , evt_index
        , from_hex(json_extract_scalar(o, '$.token')) as token
        , cast(sum(cast(json_extract_scalar(o, '$.amount') as uint256)) as uint256) as amount
    from orders
    cross join unnest(offer) as t(o)
    where json_extract_scalar(o, '$.itemType') = '1' -- ERC20 only
    group by tx_hash, evt_index, from_hex(json_extract_scalar(o, '$.token'))
)

-- token the taker PAYS (offerer receives, incl. fees) -> token_sold
, consideration_side as (
    select
        tx_hash
        , evt_index
        , from_hex(json_extract_scalar(c, '$.token')) as token
        , cast(sum(cast(json_extract_scalar(c, '$.amount') as uint256)) as uint256) as amount
    from orders
    cross join unnest(consideration) as t(c)
    where json_extract_scalar(c, '$.itemType') = '1' -- ERC20 only
    group by tx_hash, evt_index, from_hex(json_extract_scalar(c, '$.token'))
)

select
    'hyperevm' as blockchain
    , 'agra' as project
    , '1' as version
    , cast(date_trunc('month', o.block_time) as date) as block_month
    , cast(date_trunc('day', o.block_time) as date) as block_date
    , o.block_time
    , o.block_number
    , ofr.amount as token_bought_amount_raw
    , con.amount as token_sold_amount_raw
    , ofr.token as token_bought_address
    , con.token as token_sold_address
    , o.recipient as taker
    , o.offerer as maker
    , o.project_contract_address
    , o.tx_hash
    , o.evt_index
from orders o
inner join offer_side ofr
    on ofr.tx_hash = o.tx_hash
    and ofr.evt_index = o.evt_index
inner join consideration_side con
    on con.tx_hash = o.tx_hash
    and con.evt_index = o.evt_index
left join matched_hashes m
    on m.tx_hash = o.tx_hash
    and m.order_hash = o.order_hash
-- drop the duplicated taker aggregate leg of matched orders; keep everything else
where m.order_hash is null or o.offerer <> o.recipient
