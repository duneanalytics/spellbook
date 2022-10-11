{{ config(
    alias = 'events',
    partition_by = ['block_time'],
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
            , `_tokenId`
            , `_seller`
            , `_buyer`
            , `_amount`
            , evt_tx_hash
            , '' as `_currencyAddress`
    -- from superrare_ethereum.SuperRareMarketAuction_evt_Sold
    from {{ source('superrare_ethereum','SuperRareMarketAuction_evt_Sold') }}
    {% if is_incremental() %} -- this filter will only be applied on an incremental run
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
            , '' as `_currencyAddress`
    -- from superrare_ethereum.SuperRare_evt_Sold
    from {{ source('superrare_ethereum','SuperRare_evt_Sold') }}
    {% if is_incremental() %} -- this filter will only be applied on an incremental run
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
            , '' as `_currencyAddress`
    -- from superrare_ethereum.SuperRareMarketAuction_evt_AcceptBid
    from {{ source('superrare_ethereum','SuperRareMarketAuction_evt_AcceptBid') }}
    {% if is_incremental() %} -- this filter will only be applied on an incremental run
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
            , '' as `_currencyAddress`
    -- from superrare_ethereum.SuperRare_evt_AcceptBid
    from {{ source('superrare_ethereum','SuperRare_evt_AcceptBid') }}
    {% if is_incremental() %} -- this filter will only be applied on an incremental run
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
    -- from superrare_ethereum.SuperRareBazaar_evt_AcceptOffer
    from {{ source('superrare_ethereum','SuperRareBazaar_evt_AcceptOffer') }}
    {% if is_incremental() %} -- this filter will only be applied on an incremental run
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
    -- from superrare_ethereum.SuperRareBazaar_evt_AuctionSettled
    from {{ source('superrare_ethereum','SuperRareBazaar_evt_AuctionSettled') }}
    {% if is_incremental() %} -- this filter will only be applied on an incremental run
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
    -- from superrare_ethereum.SuperRareBazaar_evt_Sold
    from {{ source('superrare_ethereum','SuperRareBazaar_evt_Sold') }}
    {% if is_incremental() %} -- this filter will only be applied on an incremental run
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
            , '' as `_currencyAddress`
    -- from ethereum.logs
    from {{ source('ethereum','logs') }}
    where contract_address = lower('0x8c9f364bf7a56ed058fc63ef81c6cf09c833e656')
        and topic1 = lower('0xea6d16c6bfcad11577aef5cc6728231c9f069ac78393828f8ca96847405902a9')
        {% if is_incremental() %} -- this filter will only be applied on an incremental run
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
            , '' as `_currencyAddress`
    -- from ethereum.logs
    from {{ source('ethereum','logs') }}
    where contract_address =  lower('0x65b49f7aee40347f5a90b714be4ef086f3fe5e2c')
        and topic1 in (lower('0x2a9d06eec42acd217a17785dbec90b8b4f01a93ecd8c127edd36bfccf239f8b6')
                        , lower('0x5764dbcef91eb6f946584f4ea671217c686fa7e858ce4f9f42d08422b86556a9')
                      )
        {% if is_incremental() %} -- this filter will only be applied on an incremental run
        and block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
)

-- some items are sold in RARE currency on superrare. not available on coinpaprika, so using dex data to be able to convert to USD. usuing weekly average since dex data isn't fully populated on v2 right now (~1/8 of data vs. v1). switch back to daily once full data is available
, rare_token_price_eth as ( 
    with all_rare_eth_dex_trades as (
        select  block_time
                , token_bought_symbol
                , token_sold_symbol
                , token_bought_amount
                , token_sold_amount 
                , case when token_bought_symbol like '%ETH%' then token_bought_amount*1.0/nullif(token_sold_amount,0) 
                    else  token_sold_amount*1.0/nullif(token_bought_amount,0)
                end as eth_per_rare
        -- from dex.trades
        from {{ ref('dex_trades') }}
        where  -- RARE trades
               (token_bought_address = lower('0xba5bde662c17e2adff1075610382b9b691296350') or token_sold_address = lower('0xba5bde662c17e2adff1075610382b9b691296350'))
                and (token_bought_symbol like '%ETH%' or token_sold_symbol like '%ETH%')   
            {% if is_incremental() %} -- this filter will only be applied on an incremental run
            and block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        
        order by block_time desc 
    )
    , all_days as (
        select explode(sequence(to_date((select min(date_trunc('week',block_time)) from all_rare_eth_dex_trades)), to_date(now()), interval 1 week)) as week
    )
    
    select a.week
            , average_price_that_day_eth_per_rare
    from all_days a 
    left outer join (
        select date_trunc('week',block_time) as week
                , avg(eth_per_rare) as average_price_that_day_eth_per_rare
        from (select * from all_rare_eth_dex_trades where eth_per_rare < 0.001) a
        group by 1 
        ) b 
    on a.week = b.week
)

-- Mint details needed to determine whether a transaction was a primary or secondary sale. 
-- Sometimes primary sales are not sold directly from the artist's address (e.g. sold from Gallery)
, mint_address_details_per_token_id as (
    select  contract_address
            , `tokenId`
            , a.`to` as mint_sent_to
            , b.`from` as mint_created_by
    from 
    (
    select contract_address
            , `tokenId`
            , evt_tx_hash
            , `to` 
    -- from erc721_ethereum.evt_Transfer
    from {{ source('erc721_ethereum','evt_transfer') }}
    where contract_address in (select distinct contract_address from all_superrare_sales)
    and `tokenId` in (select distinct `_tokenId` from all_superrare_sales)
    and `from` = '0x0000000000000000000000000000000000000000'
    {% if is_incremental() %} -- this filter will only be applied on an incremental run
    and evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    ) a 
    inner join 
    ( select `from` 
            , hash 
    -- from ethereum.transactions
    from {{ source('ethereum','transactions') }}
    {% if is_incremental() %} -- this filter will only be applied on an incremental run
    where block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    ) b 
    on a.evt_tx_hash = b.hash
)
, mint_address_per_token_20 as (
    select contract_address
            , `value` as `tokenId`
            , `to` as mint_address 
    -- from erc20_ethereum.evt_Transfer
    from {{ source('erc20_ethereum','evt_transfer') }}
    where contract_address in (select distinct contract_address from all_superrare_sales)
    and `value` in (select distinct `_tokenId` from all_superrare_sales)
    and `from` = '0x0000000000000000000000000000000000000000'
    {% if is_incremental() %} -- this filter will only be applied on an incremental run
    and evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

)
, token_sold_from_auction as (
    select contract_address
            , `_tokenId`
    from all_superrare_sales
    where `_seller` = lower('0x8c9f364bf7a56ed058fc63ef81c6cf09c833e656')
)
, transfers_for_tokens_sold_from_auction as (
        select contract_address
            , `tokenId`
            , evt_tx_hash
            , `to` 
            , `from`
            , lag(`from`) OVER (PARTITION BY contract_address, `tokenId` ORDER BY evt_block_time asc) as previous_owner
            , case  when `from` = lower('0x8c9f364bf7a56ed058fc63ef81c6cf09c833e656')
                        then 'From Auction House'
                    when `to` = lower('0x8c9f364bf7a56ed058fc63ef81c6cf09c833e656')
                        then 'To Auction House'
                else '' end as auction_house_flag
            , ROW_NUMBER() OVER (PARTITION BY contract_address, `tokenId` ORDER BY evt_block_time DESC) AS transaction_rank
    -- from erc721_ethereum.evt_Transfer
    from {{ source('erc721_ethereum','evt_transfer') }}
    where contract_address in (select distinct contract_address from token_sold_from_auction)
    and `tokenId` in (select distinct `_tokenId` from token_sold_from_auction)
    {% if is_incremental() %} -- this filter will only be applied on an incremental run
    and evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}    
    order by 1, 2, 8
)


SELECT  'ethereum' as blockchain,
        'superrare' as project,
        'v1' as version,
        a.evt_block_time as block_time,
        a.`_tokenId` as token_id,
        '' as collection,
        case when a.`_currencyAddress` = '0xba5bde662c17e2adff1075610382b9b691296350'
                then (a.`_amount`/1e18)*average_price_that_day_eth_per_rare*ep.price
            else (a.`_amount`/1e18)*ep.price end as amount_usd,
        case when a.`contract_address` = '0x41a322b28d0ff354040e2cbc676f0320d8c8850d'
                then 'erc20'
            else 'erc721' end as token_standard,
        'Single Item Trade' as trade_type,
        1 as number_of_items,
        'Buy' as trade_category, 
        'Trade' as evt_type,
        a.`_seller` as seller,
        a.`_buyer` as buyer,
        (a.`_amount`/1e18) as amount_original,
        a.`_amount` as amount_raw,
        case when a.`_currencyAddress` = '0xba5bde662c17e2adff1075610382b9b691296350'
                then 'RARE'
            else 'ETH' -- only RARE and ETH possible 
            end as currency_symbol,
        case when a.`_currencyAddress` = '0xba5bde662c17e2adff1075610382b9b691296350'
                then '0xba5bde662c17e2adff1075610382b9b691296350' -- only RARE and ETH possible
            else '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' end as currency_contract,
        a.contract_address as nft_contract_address,
        '' as project_contract_address, 
        '' as aggregator_name,
        '' as aggregator_address, 
        a.evt_tx_hash as tx_hash,
        t.block_number as block_number,
        t.`from` as tx_from,
        t.`to` as tx_to,

        -- SuperRare platform fee notes:
        -- Primary sales have a 3% marketplace fee on top of the listed sale price 
        -- Secondary sales also have the 3% fee on top of sale price -- but a decaying % of this goes to SR each txn, over time a portion will go to previous collectors
        -- https://help.superrare.com/en/articles/5482222-what-are-royalties-how-do-they-work
        -- For simplicity and to align with other marketplaces, this code uses the 3% aggregate marketplace fee that applies to all SR transactions and does not differentiate based on who receives it (SuperRare and/or collectors)

        ROUND((3*(a.`_amount`)/100),7) as platform_fee_amount_raw,
        ROUND((3*((a.`_amount`/1e18))/100),7) platform_fee_amount,
        case when a.`_currencyAddress` = '0xba5bde662c17e2adff1075610382b9b691296350'
                then ROUND((3*((a.`_amount`/1e18)*average_price_that_day_eth_per_rare*ep.price)/100),7)
            else ROUND((3*((a.`_amount`/1e18)*ep.price)/100),7) end as platform_fee_amount_usd,
        '3' as platform_fee_percentage,

        -- SuperRare royalty fee notes:
        -- This query aggregates both primary sales and secondary sales on the platform
        -- Primary sales -- 85% of listed sale price goes to the artist
        -- Secondary sales -- 10% royalty on listed sale price goes to the artist
        -- For simplicity and to align with other marketplaces, this code shows a 10% royalty for all secondary sales 

        case  when ma721.mint_sent_to = po.previous_owner then 'Primary' -- auctions
                when ma721.mint_sent_to = `_seller` then 'Primary'
                when ma20.mint_address = `_seller` then 'Primary'
            else 'Secondary' end as superrare_sale_type,

        case when ma721.mint_sent_to != po.previous_owner and ma721.mint_sent_to != `_seller` and ma20.mint_address != `_seller` -- secondary sale 
                then ROUND((10*(a.`_amount`)/100),7) 
            else null end as royalty_fee_amount_raw,
        case when ma721.mint_sent_to != po.previous_owner and ma721.mint_sent_to != `_seller` and ma20.mint_address != `_seller` -- secondary sale 
                then ROUND((10*((a.`_amount`/1e18))/100),7)
            else null end as royalty_fee_amount,
        case when a.`_currencyAddress` = '0xba5bde662c17e2adff1075610382b9b691296350' and ma721.mint_sent_to != po.previous_owner and ma721.mint_sent_to != `_seller` and ma20.mint_address != `_seller` -- secondary sale and rare
                then ROUND((10*((a.`_amount`/1e18)*average_price_that_day_eth_per_rare*ep.price)/100),7)
             when ma721.mint_sent_to != po.previous_owner and ma721.mint_sent_to != `_seller` and ma20.mint_address != `_seller` -- secondary sales 
                then ROUND((10*((a.`_amount`/1e18)*ep.price)/100),7)
            else null end as royalty_fee_amount_usd,
        '10' as royalty_fee_percentage,
        case when ma721.mint_sent_to is not null 
                then ma721.mint_sent_to
            else ma20.mint_address end as royalty_fee_receive_address,
        case when a.`_currencyAddress` = '0xba5bde662c17e2adff1075610382b9b691296350'
                then 'RARE'
            else 'ETH' -- only RARE and ETH possible 
            end as royalty_fee_currency_symbol,
        'superrare' || '-' || a.evt_tx_hash || '-' ||  a.`_tokenId`::string || '-' ||  `_seller`::string || '-' || COALESCE(a.contract_address) || '-' || 'Trade'  as unique_trade_id

from all_superrare_sales a

left outer join (select minute, price 
                -- from prices.usd 
                from {{ source('prices','usd') }}
                where blockchain = 'ethereum' and symbol = 'WETH' 
                {% if is_incremental() %} -- this filter will only be applied on an incremental run
                and minute >= date_trunc("day", now() - interval '1 week')
                {% endif %}
                ) ep 
on date_trunc('minute', a.evt_block_time) = ep.minute

left outer join rare_token_price_eth rp
on date_trunc('week', a.evt_block_time) = rp.week

-- left outer join ethereum.transactions t 
left outer join  {{ source('ethereum','transactions') }} t
on a.evt_tx_hash = t.hash
{% if is_incremental() %} -- this filter will only be applied on an incremental run
and t.block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}

left outer join mint_address_details_per_token_id ma721 -- erc721 - who minted and where it got sent 
on a.contract_address = ma721.contract_address
and a.`_tokenId` = ma721.`tokenId`

left outer join mint_address_per_token_20 ma20
on a.contract_address = ma20.contract_address
and a.`_tokenId` = ma20.`tokenId`

left outer join transfers_for_tokens_sold_from_auction po -- if sold from auction house previous owner 
on a.evt_tx_hash = po.evt_tx_hash

where (a.`_amount`/1e18) > 0 
order by evt_block_time desc