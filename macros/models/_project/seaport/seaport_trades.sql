{% macro seaport_trades(seaport_orders) %}
WITH filter as (
select * from (
    select *
    ,any_match(offer
        ,x -> array_contains(
                    ARRAY['erc721','erc721','erc1155','erc721_with_criteria','erc1155_with_criteria']
                    ,x.item_type)
               ) as is_buy  -- the order that was filled was a sell order
    ,any_match(consideration
        ,x -> array_contains(
                    ARRAY['erc721','erc721','erc1155','erc721_with_criteria','erc1155_with_criteria']
                    ,x.item_type)
               ) as is_sell -- the order that was filled was a buy order
    ,matched_evt_index is null as is_matched
    from {{seaport_orders}} o
)
where not (is_buy = is_sell) -- exclude complex orders where both sides contain NFTs
)



)
    o.contract_address,
    o.evt_tx_hash,
    o.evt_index,
    o.evt_block_time,
    o.evt_block_number,
    transform(o.consideration, x -> {{convert_consideration('x')}}) as consideration,
    transform(o.offer, x -> {{convert_offer('x')}}) as offer,
    o.offerer,    -- offers the items in offer[] and if consideration[] is fulfilled
    o.recipient,  -- receives the items in offer[] and gives the consideration[] items to fill the order
    o.orderHash,
    o.zone,
    null as ordersmatched_evt_index,
    null as matched_evt_index,
    null as matched_consideration,
    null as matched_offer,
    null as matched_offerer,    -- offers the items in offer[] and if consideration[] is fulfilled
    null as matched_recipient,  -- receives the items in offer[] and gives the consideration[] items to fill the order
    null as matched_orderHash,
    null as matched_zone
    from {{seaport_orders}} o
    left join {{Seaport_evt_OrdersMatched}} anti -- anti join so we don't overlap with any matched orders
        on o.evt_block_number = anti.evt_block_number
        and o.evt_tx_hash = anti.evt_tx_hash
        and contains(anti.orderHashes, o.orderHash)
    where anti.evt_index is null
{% endmacro %}
