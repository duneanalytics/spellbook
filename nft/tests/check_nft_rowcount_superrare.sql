with unit_test as (

select count_a, count_b, abs(count_a - count_b) as diff_count
from
    (select count(*) as count_a
     from {{ ref('superrare_ethereum_base_trades')}}
     )
full outer join
    (select sum(count_b) as count_b
    from (
        select count(*) as count_b from {{ source('superrare_ethereum','SuperRareMarketAuction_evt_Sold') }}
        where evt_block_time <= (select max(block_time) from {{ ref('superrare_ethereum_base_trades')}})
        union all
        select count(*) as count_b from {{ source('superrare_ethereum','SuperRare_evt_Sold') }}
        where evt_block_time <= (select max(block_time) from {{ ref('superrare_ethereum_base_trades')}})
        union all
        select count(*) as count_b from {{ source('superrare_ethereum','SuperRareMarketAuction_evt_AcceptBid') }}
        where evt_block_time <= (select max(block_time) from {{ ref('superrare_ethereum_base_trades')}})
        union all
        select count(*) as count_b from {{ source('superrare_ethereum','SuperRare_evt_AcceptBid') }}
        where evt_block_time <= (select max(block_time) from {{ ref('superrare_ethereum_base_trades')}})
        union all
        select count(*) as count_b from {{ source('superrare_ethereum','SuperRareBazaar_evt_AcceptOffer') }}
        where evt_block_time <= (select max(block_time) from {{ ref('superrare_ethereum_base_trades')}})
        union all
        select count(*) as count_b from {{ source('superrare_ethereum','SuperRareBazaar_evt_AuctionSettled') }}
        where evt_block_time <= (select max(block_time) from {{ ref('superrare_ethereum_base_trades')}})
        union all
        select count(*) as count_b from {{ source('superrare_ethereum','SuperRareBazaar_evt_Sold') }}
        where evt_block_time <= (select max(block_time) from {{ ref('superrare_ethereum_base_trades')}})
        union all
        select count(*) as count_b from {{ source('superrare_ethereum','SuperRareAuctionHouse_evt_AuctionSettled') }}
        where evt_block_time <= (select max(block_time) from {{ ref('superrare_ethereum_base_trades')}})
        )
    )
on 1=1
)

select * from unit_test where diff_count > 0

