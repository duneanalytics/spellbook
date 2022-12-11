{{ config(
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'unique_trade_id'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "superrare",
                                \'["cat"]\') }}'
    )
}}

-- raw data table with all sales on superrare platform -- both primary and secondary 
with all_superrare_sales as (
    select  evt_block_time
            , `_originContract` as contract_address
            , `_tokenId` as tokenId
            , `_seller` as seller
            , `_buyer` as buyer
            , `_amount` as amount
            , evt_tx_hash
            , '' as currencyAddress
    from {{ source('superrare_ethereum','SuperRareMarketAuction_evt_Sold') }}
    {% if is_incremental() %}
    where evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    union all 
    
    select evt_block_time
            , contract_address
            , `_tokenId`
            , `_seller`
            , `_buyer`
            , `_amount`
            , evt_tx_hash  
            , ''
    from {{ source('superrare_ethereum','SuperRare_evt_Sold') }}
    {% if is_incremental() %}
    where evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    union all 
    
    select evt_block_time
            , `_originContract` as contract_address
            , `_tokenId`
            , `_seller`
            , `_bidder` 
            , `_amount`
            , evt_tx_hash 
            , ''
    from {{ source('superrare_ethereum','SuperRareMarketAuction_evt_AcceptBid') }}
    {% if is_incremental() %}
    where evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}    

    union all 
    
    select evt_block_time
            , contract_address
            , `_tokenId`
            , `_seller`
            , `_bidder` 
            , `_amount`
            , evt_tx_hash 
            , ''
    from {{ source('superrare_ethereum','SuperRare_evt_AcceptBid') }}
    {% if is_incremental() %}
    where evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    union all 
    
    select evt_block_time
            , `_originContract`
            , `_tokenId`
            , `_seller`
            , `_bidder` 
            , `_amount`
            , evt_tx_hash 
            , `_currencyAddress`
    from {{ source('superrare_ethereum','SuperRareBazaar_evt_AcceptOffer') }}
    {% if is_incremental() %}
    where evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    union all 
    
    select evt_block_time
            , `_contractAddress`
            , `_tokenId`
            , `_seller`
            , `_bidder` 
            , `_amount`
            , evt_tx_hash 
            , `_currencyAddress`
    from {{ source('superrare_ethereum','SuperRareBazaar_evt_AuctionSettled') }}
    {% if is_incremental() %}
    where evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    union all 
    
    select evt_block_time
            , `_originContract`
            , `_tokenId`
            , `_seller`
            , `_buyer`
            , `_amount`
            , evt_tx_hash 
            , `_currencyAddress`
    from {{ source('superrare_ethereum','SuperRareBazaar_evt_Sold') }}
    {% if is_incremental() %}
    where evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    union all 
    
    select  block_time
            , concat('0x',substring(topic2 from 27 for 40)) as contract_address 
            , bytea2numeric_v2(substring(topic4 from 3)) as token_id 
            , lower('0x8c9f364bf7a56ed058fc63ef81c6cf09c833e656') as seller -- all sent from auction house contract 
            , concat('0x',substring(topic3 from 27 for 40)) as buyer 
            , bytea2numeric_v2(substring(data from 67 for 64)) as amount
            , tx_hash
            , ''
    from {{ source('ethereum','logs') }}
    where contract_address = lower('0x8c9f364bf7a56ed058fc63ef81c6cf09c833e656')
        and topic1 = lower('0xea6d16c6bfcad11577aef5cc6728231c9f069ac78393828f8ca96847405902a9')
        {% if is_incremental() %}
        and block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}

    union all 
    
    select block_time
            , concat('0x',substring(topic2 from 27 for 40)) as contract_address 
            , bytea2numeric_v2(substring(data from 67 for 64)) as token_id
            , concat('0x',substring(topic4 from 27 for 40)) as seller 
            , concat('0x',substring(topic3 from 27 for 40)) as buyer 
            , bytea2numeric_v2(substring(data from 3 for 64)) as amount
            , tx_hash
            , ''
    from {{ source('ethereum','logs') }}
    where contract_address =  lower('0x65b49f7aee40347f5a90b714be4ef086f3fe5e2c')
        and topic1 in (lower('0x2a9d06eec42acd217a17785dbec90b8b4f01a93ecd8c127edd36bfccf239f8b6')
                        , lower('0x5764dbcef91eb6f946584f4ea671217c686fa7e858ce4f9f42d08422b86556a9')
                      )
        {% if is_incremental() %}
        and block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
)
-- some items are sold in RARE currency on superrare. not available on coinpaprika, so using dex data to be able to convert to USD. usuing weekly average since dex data isn't fully populated on v2 right now (~1/8 of data vs. v1). switch back to daily once full data is available
, rare_token_price_eth as (
    select
        date_trunc('week', block_time) as week,
        avg(
            case
            when token_bought_symbol like '%ETH%' then token_bought_amount * 1.0 / nullif(token_sold_amount, 0)
            else token_sold_amount * 1.0 / nullif(token_bought_amount, 0)
            end
        ) as average_price_that_day_eth_per_rare
    from
        {{ ref('dex_trades') }}
    where
        -- RARE trades
        blockchain = 'ethereum'
        and (
            token_bought_address = lower('0xba5bde662c17e2adff1075610382b9b691296350')
            or token_sold_address = lower('0xba5bde662c17e2adff1075610382b9b691296350')
        )
        and (
            token_bought_symbol like '%ETH%'
            or token_sold_symbol like '%ETH%'
        )
        and case
            when token_bought_symbol like '%ETH%' then token_bought_amount * 1.0 / nullif(token_sold_amount, 0)
            else token_sold_amount * 1.0 / nullif(token_bought_amount, 0)
            end < 0.001
        {% if is_incremental() %}
        and block_date >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    group by
        1
)
, transfers_for_tokens_sold_from_auction as (
        select evt.contract_address
            , evt.tokenId
            , evt.evt_tx_hash
            , evt.to
            , evt.from
            , lag(evt.from) OVER (PARTITION BY evt.contract_address, evt.tokenId ORDER BY evt.evt_block_time asc) as previous_owner
            , case  when evt.from = lower('0x8c9f364bf7a56ed058fc63ef81c6cf09c833e656')
                        then 'From Auction House'
                    when evt.to = lower('0x8c9f364bf7a56ed058fc63ef81c6cf09c833e656')
                        then 'To Auction House'
                else '' end as auction_house_flag
            , ROW_NUMBER() OVER (PARTITION BY evt.contract_address, evt.tokenId ORDER BY evt.evt_block_time DESC) AS transaction_rank
    from {{ source('erc721_ethereum','evt_transfer') }} evt
    inner join all_superrare_sales tsfa
        on evt.contract_address = tsfa.contract_address
        and evt.tokenId = tsfa.tokenId
        and tsfa.seller = lower('0x8c9f364bf7a56ed058fc63ef81c6cf09c833e656')
    {% if is_incremental() %}
    where evt.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)
SELECT 
    'ethereum' as blockchain,
    'superrare' as project,
    'v1' as version,
    cast(date_trunc('day', a.evt_block_time) AS date) AS block_date,
    a.evt_block_time as block_time,
    a.tokenId as token_id,
    '' as collection,
    case
    when a.currencyAddress = '0xba5bde662c17e2adff1075610382b9b691296350' then (a.amount / 1e18) * average_price_that_day_eth_per_rare * ep.price
    else (a.amount / 1e18) * ep.price
    end as amount_usd,
    case
    when a.contract_address = '0x41a322b28d0ff354040e2cbc676f0320d8c8850d' then 'erc20'
    else 'erc721'
    end as token_standard,
    'Single Item Trade' as trade_type,
    CAST(1 AS DECIMAL(38,0)) as number_of_items,
    'Buy' as trade_category,
    'Trade' as evt_type,
    a.seller as seller,
    a.buyer as buyer,
    (a.amount / 1e18) as amount_original,
    CAST(a.amount AS DECIMAL(38,0)) as amount_raw,
    case
    when a.currencyAddress = '0xba5bde662c17e2adff1075610382b9b691296350' then 'RARE'
    else 'ETH' -- only RARE and ETH possible
    end as currency_symbol,
    case
    when a.currencyAddress = '0xba5bde662c17e2adff1075610382b9b691296350' then '0xba5bde662c17e2adff1075610382b9b691296350'
    else '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    end as currency_contract,
    a.contract_address as nft_contract_address,
    '' as project_contract_address,
    '' as aggregator_name,
    '' as aggregator_address,
    a.evt_tx_hash as tx_hash,
    t.block_number as block_number,
    t.from as tx_from,
    t.to as tx_to,
    ROUND((3 * (a.amount) / 100), 7) as platform_fee_amount_raw,
    ROUND((3 * ((a.amount / 1e18)) / 100), 7) platform_fee_amount,
    case
    when a.currencyAddress = '0xba5bde662c17e2adff1075610382b9b691296350' then ROUND(
        (
        3 * (
            (a.amount / 1e18) * average_price_that_day_eth_per_rare * ep.price
        ) / 100
        ),
        7
    )
    else ROUND((3 * ((a.amount / 1e18) * ep.price) / 100), 7)
    end as platform_fee_amount_usd,
    CAST('3' AS DOUBLE) as platform_fee_percentage,
    case
    when evt.to = po.previous_owner then 'Primary' -- auctions
    when evt.to = seller then 'Primary'
    when erc20.to = seller then 'Primary'
    else 'Secondary'
    end as superrare_sale_type,
    case
    when evt.to != po.previous_owner
    and evt.to != seller
    and erc20.to != seller -- secondary sale
    then ROUND((10 * (a.amount) / 100), 7)
    else null
    end as royalty_fee_amount_raw,
    case
    when evt.to != po.previous_owner
    and evt.to != seller
    and erc20.to != seller -- secondary sale
    then ROUND((10 * ((a.amount / 1e18)) / 100), 7)
    else null
    end as royalty_fee_amount,
    case
    when a.currencyAddress = '0xba5bde662c17e2adff1075610382b9b691296350'
    and evt.to != po.previous_owner
    and evt.to != seller
    and erc20.to != seller -- secondary sale and rare
    then ROUND(
        (
        10 * (
            (a.amount / 1e18) * average_price_that_day_eth_per_rare * ep.price
        ) / 100
        ),
        7
    )
    when evt.to != po.previous_owner
    and evt.to != seller
    and erc20.to != seller -- secondary sales
    then ROUND((10 * ((a.amount / 1e18) * ep.price) / 100), 7)
    else null
    end as royalty_fee_amount_usd,
    CAST('10' AS DOUBLE) as royalty_fee_percentage,
    case
    when evt.to is not null then evt.to
    else erc20.to
    end as royalty_fee_receive_address,
    case
    when a.currencyAddress = '0xba5bde662c17e2adff1075610382b9b691296350' then 'RARE'
    else 'ETH' -- only RARE and ETH possible
    end as royalty_fee_currency_symbol,
    'superrare' || '-' || a.evt_tx_hash || '-' || CAST(a.tokenId AS VARCHAR(100)) || '-' || CAST(a.seller AS VARCHAR(100)) || '-' || COALESCE(a.contract_address) || '-' || 'Trade' as unique_trade_id
from all_superrare_sales a
left outer join
    (
        select
            minute
            , price
        from {{ source('prices','usd') }}
        where blockchain = 'ethereum'
            and symbol = 'WETH' 
            {% if is_incremental() %}
            and minute >= date_trunc("day", now() - interval '1 week')
            {% endif %}
    ) ep 
    on date_trunc('minute', a.evt_block_time) = ep.minute
left outer join rare_token_price_eth rp
    on date_trunc('week', a.evt_block_time) = rp.week
inner join {{ source('ethereum','transactions') }} t
    on a.evt_tx_hash = t.hash
    {% if is_incremental() %}
    and t.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
left outer join {{ source('erc721_ethereum','evt_transfer') }} evt on evt.contract_address = a.contract_address
    and evt.tokenId = a.tokenId
    and evt.from = '0x0000000000000000000000000000000000000000'
    {% if is_incremental() %}
    and evt.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
left outer join {{ source('erc20_ethereum','evt_transfer') }} erc20 on erc20.contract_address = a.contract_address
    and erc20.value = a.tokenId
    and erc20.from = '0x0000000000000000000000000000000000000000'
    {% if is_incremental() %}
    and erc20.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
left outer join transfers_for_tokens_sold_from_auction po -- if sold from auction house previous owner 
    on a.evt_tx_hash = po.evt_tx_hash
where (a.amount/1e18) > 0
;