{{ config(
    alias = 'trades',
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
    from superrare_ethereum.SuperRareMarketAuction_evt_Sold

    union all 
    
    select evt_block_time
            , contract_address
            , `_tokenId`
            , `_seller`
            , `_buyer`
            , `_amount`
            , evt_tx_hash  
            , '' as `_currencyAddress`
    from superrare_ethereum.SuperRare_evt_Sold

    union all 
    
    select evt_block_time
            , `_originContract` as contract_address
            , `_tokenId`
            , `_seller`
            , `_bidder` 
            , `_amount`
            , evt_tx_hash 
            , '' as `_currencyAddress`
    from superrare_ethereum.SuperRareMarketAuction_evt_AcceptBid
      
    union all 
    
    select evt_block_time
            , contract_address
            , `_tokenId`
            , `_seller`
            , `_bidder` 
            , `_amount`
            , evt_tx_hash 
            , '' as `_currencyAddress`
    from superrare_ethereum.SuperRare_evt_AcceptBid
    
    union all 
    
    select evt_block_time
            , `_originContract`
            , `_tokenId`
            , `_seller`
            , `_bidder` 
            , `_amount`
            , evt_tx_hash 
            , `_currencyAddress`
    from superrare_ethereum.SuperRareBazaar_evt_AcceptOffer

    union all 
    
    select evt_block_time
            , `_contractAddress`
            , `_tokenId`
            , `_seller`
            , `_bidder` 
            , `_amount`
            , evt_tx_hash 
            , `_currencyAddress`
    from superrare_ethereum.SuperRareBazaar_evt_AuctionSettled

    union all 
    
    select evt_block_time
            , `_originContract`
            , `_tokenId`
            , `_seller`
            , `_buyer`
            , `_amount`
            , evt_tx_hash 
            , `_currencyAddress`
    from superrare_ethereum.SuperRareBazaar_evt_Sold

    union all 
    
    select  block_time
            , concat('0x',substring(topic2 from 27 for 40)) as contract_address 
            , bytea2numeric_v2(substring(topic4 from 3)) as token_id 
            , lower('0x8c9f364bf7a56ed058fc63ef81c6cf09c833e656') as seller -- all sent from auction house contract 
            , concat('0x',substring(topic3 from 27 for 40)) as buyer 
            , bytea2numeric_v2(substring(data from 67 for 64)) as amount
            , tx_hash
            , '' as `_currencyAddress`
    from ethereum.logs
    where contract_address = lower('0x8c9f364bf7a56ed058fc63ef81c6cf09c833e656')
        and topic1 = lower('0xea6d16c6bfcad11577aef5cc6728231c9f069ac78393828f8ca96847405902a9')

    union all 
    
    select block_time
            , concat('0x',substring(topic2 from 27 for 40)) as contract_address 
            , bytea2numeric_v2(substring(data from 67 for 64)) as token_id
            , concat('0x',substring(topic4 from 27 for 40)) as seller 
            , concat('0x',substring(topic3 from 27 for 40)) as buyer 
            , bytea2numeric_v2(substring(data from 3 for 64)) as amount
            , tx_hash
            , '' as `_currencyAddress`
    from ethereum.logs
    where contract_address = lower('0x65b49f7aee40347f5a90b714be4ef086f3fe5e2c')
        and topic1 in (lower('0x2a9d06eec42acd217a17785dbec90b8b4f01a93ecd8c127edd36bfccf239f8b6')
                        , lower('0x5764dbcef91eb6f946584f4ea671217c686fa7e858ce4f9f42d08422b86556a9'))

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
        from dex.trades
        where   -- RARE trades
                (token_bought_address = lower('0xba5bde662c17e2adff1075610382b9b691296350') or token_sold_address = lower('0xba5bde662c17e2adff1075610382b9b691296350'))
            and (token_bought_symbol like '%ETH%' or token_sold_symbol like '%ETH%')    
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
        'superrare' || '-' || a.evt_tx_hash || '-' ||  a.`_tokenId`::string || '-' ||  `_seller`::string || '-' || COALESCE(a.contract_address) || '-' || 'Trade'  as unique_trade_id

from all_superrare_sales a

left outer join (select minute, price from prices.usd where blockchain = 'ethereum' and symbol = 'WETH' ) ep 
on date_trunc('minute', a.evt_block_time) = ep.minute

left outer join rare_token_price_eth rp
on date_trunc('week', a.evt_block_time) = rp.week

left outer join ethereum.transactions t 
on a.evt_tx_hash = t.hash

where (a.`_amount`/1e18) > 0 
order by evt_block_time desc
limit 20
