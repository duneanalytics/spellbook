{{ config(
        schema = 'cryptopunks_ethereum',
        alias ='events',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_trade_id'
        )
}}

with cryptopunks_bids_and_sales as (
    select *
            , row_number() over (partition by punk_id order by evt_block_number asc, evt_index asc) as punk_id_event_number
    from
    (
    select  "PunkBought" as event_type
            , punkIndex as punk_id
            , cast(value/1e18 as double) as sale_price
            , cast(NULL as double) as bid_amount
            , toAddress as to_address
            , fromAddress as from_address
            , cast(NULL as varchar(5)) as bid_from_address
            , evt_block_number
            , evt_index
            , evt_block_time
            , evt_tx_hash
    from {{ source('cryptopunks_ethereum','CryptoPunksMarket_evt_PunkBought') }}

    union all

    select  "PunkBidEntered" as event_type
            , punkIndex as punk_id
            , cast(NULL as double) as sale_price
            , value/1e18 as bid_amount
            , cast(NULL as varchar(5)) as to_address
            , cast(NULL as varchar(5)) as from_address
            , fromAddress as bid_from_address
            , evt_block_number
            , evt_index
            , evt_block_time
            , evt_tx_hash
    from {{ source('cryptopunks_ethereum','CryptoPunksMarket_evt_PunkBidEntered') }}

    union all

    select  "PunkBidWithdrawn" as event_type
            , punkIndex as punk_id
            , cast(NULL as double) as sale_price
            , value/1e18 as bid_amount
            , cast(NULL as varchar(5)) as to_address
            , cast(NULL as varchar(5)) as from_address
            , fromAddress as bid_from_address
            , evt_block_number
            , evt_index
            , evt_block_time
            , evt_tx_hash
    from {{ source('cryptopunks_ethereum','CryptoPunksMarket_evt_PunkBidWithdrawn') }}
    ) a
)
, bid_sales as (
    select  "Offer Accepted" as event_type
            , a.punk_id
            , max(c.bid_amount) as sale_price -- max bid from buyer pre-sale
            , b.to as to_address -- for bids accepted, look up who the seller transferred to in the same block with 1 offset index
            , a.from_address
            , a.evt_block_number
            , a.evt_index
            , a.evt_block_time
            , a.evt_tx_hash
    from cryptopunks_bids_and_sales a

    join {{ source('cryptopunks_ethereum','CryptoPunksMarket_evt_Transfer') }} b
    on a.from_address = b.from and a.evt_block_number = b.evt_block_number and a.evt_index = (b.evt_index+1)

    left outer join cryptopunks_bids_and_sales c
    on a.punk_id = c.punk_id and c.event_type = "PunkBidEntered" and c.punk_id_event_number < a.punk_id_event_number and c.bid_from_address = b.to

    where a.sale_price = 0 and a.to_address = '0x0000000000000000000000000000000000000000'
    group by 1,2,4,5,6,7,8,9
)
, regular_sales as (
    select  "Buy" as event_type
            , a.punkIndex as punk_id
            , a.value/1e18 as sale_price
            , case when a.toAddress = '0x83c8f28c26bf6aaca652df1dbbe0e1b56f8baba2' -- gem
                then b.to
                else a.toAddress end as to_address
            , a.fromAddress as from_address
            , a.evt_block_number
            , a.evt_index
            , a.evt_block_time
            , a.evt_tx_hash
    from {{ source('cryptopunks_ethereum','CryptoPunksMarket_evt_PunkBought') }} a
    left outer join {{ source('cryptopunks_ethereum','CryptoPunksMarket_evt_PunkTransfer') }} b
    on a.punkIndex = b.punkIndex
        and a.toAddress = b.from
        and b.from = '0x83c8f28c26bf6aaca652df1dbbe0e1b56f8baba2'
        and a.evt_tx_hash = b.evt_tx_hash

    where a.value != 0 or a.toAddress != '0x0000000000000000000000000000000000000000' -- only include sales here
)


select  "ethereum" as blockchain
        , "cryptopunks" as project
        , "v1" as version
        , a.evt_block_time as block_time
        , a.punk_id as token_id
        , "CryptoPunks" as collection
        , a.sale_price * p.price as amount_usd
        , "erc20" as token_standard
        , '' as trade_type
        , CAST(1 AS DECIMAL(38,0)) as number_of_items
        , a.event_type as trade_category
        , from_address as seller
        , to_address as buyer
        , "Trade" as evt_type
        , sale_price as amount_original
        , CAST((sale_price * 1e18) AS DECIMAL(38,0)) as amount_raw
        , "ETH" as currency_symbol
        , "0x0000000000000000000000000000000000000000" as currency_contract
        , "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb" as nft_contract_address
        , "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb" as project_contract_address
        , agg.name as aggregator_name
        , agg.contract_address as aggregator_address
        , a.evt_block_number as block_number
        , a.evt_index as evt_index
        , a.evt_tx_hash as tx_hash
        , tx.from as tx_from
        , tx.to as tx_to
        , cast(0 as double) as platform_fee_amount_raw
        , cast(0 as double) as platform_fee_amount
        , cast(0 as double) as platform_fee_amount_usd
        , cast(0 as double) as platform_fee_percentage
        , cast(0 as double) as royalty_fee_amount_raw
        , cast(0 as double) as royalty_fee_amount
        , cast(0 as double) as royalty_fee_amount_usd
        , cast(0 as double) as royalty_fee_percentage
        , '' as royalty_fee_receive_address
        , '' as royalty_fee_currency_symbol
        , "cryptopunks" || '-' || a.evt_tx_hash || '-' || a.punk_id || '-' ||  a.from_address || '-' || a.evt_index || '-' || "" as unique_trade_id
from
(   select * from bid_sales
    union all
    select * from regular_sales
) a

inner join {{ source('ethereum','transactions') }} tx on a.evt_tx_hash = tx.hash
{% if is_incremental() %}
    and tx.block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}

left join {{ source('prices', 'usd') }} p on p.minute = date_trunc('minute', a.evt_block_time)
    and p.contract_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    and p.blockchain = 'ethereum'
{% if is_incremental() %}
    and p.minute >= date_trunc('day', now() - interval '1 week')
{% endif %}

left join {{ ref('nft_ethereum_aggregators') }} agg on agg.contract_address = tx.to

where a.evt_tx_hash not in ('0x92488a00dfa0746c300c66a716e6cc11ba9c0f9d40d8c58e792cc7fcebf432d0' -- flash loan https://twitter.com/cryptopunksnfts/status/1453903818308083720
                         )
