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
        select count(*) as count_b from {{ source('ethereum','logs') }}
            where contract_address = 0x8c9f364bf7a56ed058fc63ef81c6cf09c833e656
            and topic0 = 0xea6d16c6bfcad11577aef5cc6728231c9f069ac78393828f8ca96847405902a9
            and block_time <= (select max(block_time) from {{ ref('superrare_ethereum_base_trades')}})
        union all
        select count(*) as count_b from {{ source('ethereum','logs') }}
            where contract_address =  0x65b49f7aee40347f5a90b714be4ef086f3fe5e2c
            and topic0 in (0x2a9d06eec42acd217a17785dbec90b8b4f01a93ecd8c127edd36bfccf239f8b6
                          ,0x5764dbcef91eb6f946584f4ea671217c686fa7e858ce4f9f42d08422b86556a9)
            and block_time <= (select max(block_time) from {{ ref('superrare_ethereum_base_trades')}})
        )
    )
on 1=1
)

select * from unit_test where diff_count > 0

