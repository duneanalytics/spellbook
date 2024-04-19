-- utility macro's:
-- convert offer (SpentItem[]) to a ROW
{% macro convert_offer(col) %}
cast(ROW(
    case json_extract_scalar({{col}}, '$.itemType')  -- https://github.com/ProjectOpenSea/seaport/blob/main/docs/SeaportDocumentation.md#order
        when '0' then 'native'
        when '1' then 'erc20'
        when '2' then 'erc721'
        when '3' then 'erc1155'
        when '4' then 'erc721_with_criteria'
        when '5' then 'erc1155_with_criteria'
    end
    ,from_hex(json_extract_scalar({{col}}, '$.token'))
    ,cast(json_extract_scalar({{col}}, '$.identifier') as uint256)
    ,cast(json_extract_scalar({{col}}, '$.amount') as uint256))
as ROW(item_type varchar, token varbinary, identifier uint256, amount uint256))
{% endmacro %}

-- convert consideration (ReceivedItem[]) to a ROW
-- consideration
--  + item_type (varchar)
--  + token (varbin)
--  + identifier (uint256)
--  + amount (uint256)
--  + recipient ( varbin)
{% macro convert_consideration(col) %}
cast(ROW(
    case json_extract_scalar({{col}}, '$.itemType')
        when '0' then 'native'
        when '1' then 'erc20'
        when '2' then 'erc721'
        when '3' then 'erc1155'
        when '4' then 'erc721_with_criteria'
        when '5' then 'erc1155_with_criteria'
    end
    ,from_hex(json_extract_scalar({{col}}, '$.token'))
    ,cast(json_extract_scalar({{col}}, '$.identifier') as uint256)
    ,cast(json_extract_scalar({{col}}, '$.amount') as uint256)
    ,from_hex(json_extract_scalar({{col}}, '$.recipient')))
as ROW(item_type varchar, token varbinary, identifier uint256, amount uint256, recipient varbinary))
{% endmacro %}


-- this macro is used to extract all the order information out of the emitted seaport events.
-- it will NOT impose any additional assumptions on the emitted data (such as what sides is part of the sale, what side is the royalty payment, ...)
-- imposing these assumptions and reducing them to valid trade information should be done by downstream models/macro's
{% macro seaport_orders(blockchainSeaport_evt_OrderFulfilled, Seaport_evt_OrdersMatched) %}
WITH basic as (
    SELECT
        e.contract_address,
        e.evt_tx_hash,
        e.evt_index,
        e.evt_block_time,
        e.evt_block_number,
        transform(e.consideration, x -> {{convert_consideration('x')}}) as consideration,
        transform(e.offer, x -> {{convert_offer('x')}}) as offer,
        e.offerer,    -- offers the items in offer[] and if consideration[] is fulfilled
        e.recipient,  -- receives the items in offer[] and gives the consideration[] items to fill the order
        e.orderHash,
        e.zone,
        null as ordersmatched_evt_index,
        null as matched_evt_index,
        null as matched_consideration,
        null as matched_offer,
        null as matched_offerer,    -- offers the items in offer[] and if consideration[] is fulfilled
        null as matched_recipient,  -- receives the items in offer[] and gives the consideration[] items to fill the order
        null as matched_orderHash,
        null as matched_zone
    from {{Seaport_evt_OrderFulfilled}} e
    left join {{Seaport_evt_OrdersMatched}} anti -- anti join so we don't overlap with any matched orders
        on e.evt_block_number = anti.evt_block_number
        and e.evt_tx_hash = anti.evt_tx_hash
        and contains(anti.orderHashes, e.orderHash)
    where anti.evt_index is null
),

matched as (
    SELECT
        e.contract_address,
        e.evt_tx_hash,
        e.evt_index,
        e.evt_block_time,
        e.evt_block_number,
        transform(e.consideration, x -> {{convert_consideration('x')}}) as consideration,
        transform(e.offer, x -> {{convert_offer('x')}}) as offer,
        e.offerer,    -- offers the items in offer[] and if consideration[] is fulfilled
        e.recipient,  -- receives the items in offer[] and gives the consideration[] items to fill the order
        e.orderHash,
        e.zone,
        matched.evt_index as ordersmatched_evt_index,
        pe.evt_index as matched_evt_index,
        transform(pe.consideration, x -> {{convert_consideration('x')}}) as matched_consideration,
        transform(pe.offer, x -> {{convert_offer('x')}}) as matched_offer,
        pe.offerer as matched_offerer,    -- offers the items in offer[] and if consideration[] is fulfilled
        pe.recipient as matched_recipient,  -- receives the items in offer[] and gives the consideration[] items to fill the order
        pe.orderHash as matched_orderHash,
        pe.zone as matched_zone
    from {{Seaport_evt_OrderFulfilled}} e  -- event
    inner join {{Seaport_evt_OrdersMatched}} matched
        on e.evt_block_number = matched.evt_block_number
        and e.evt_tx_hash = matched.evt_tx_hash
        and contains(matched.orderHashes, e.orderHash)
    inner join {{Seaport_evt_OrderFulfilled}} pe -- paired event
        on e.evt_block_number = pe.evt_block_number
        and e.evt_tx_hash = pe.evt_tx_hash
        and e.evt_index = pe.evt_index - 1 -- events should be emitted after each other
        and contains_sequence(matched.orderHashes,ARRAY[e.orderHash,pe.orderHash])   -- matchedOrderhashes should contain the sequence of the 2 orderHashes
)

select * from basic
union all
select * from matched
{% endmacro %}




