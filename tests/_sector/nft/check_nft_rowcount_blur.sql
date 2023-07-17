with unit_test as (

select count_a, count_b, (count_a - count_b) as diff_count
from
    (select count(*) as count_a
     from {{ ref('blur_ethereum_base_trades')}}
     )
full outer join
    (select sum(count_b) as count_b
    from (
        select count(*) as count_b from {{ source('blur_ethereum','BlurExchange_evt_OrdersMatched') }}
        where evt_block_time <= (select max(block_time) from {{ ref('blur_ethereum_base_trades')}})
        union all
        select count(*) as count_b from {{ source('seaport_ethereum','Seaport_evt_OrderFulfilled') }}
        where evt_block_time <= (select max(block_time) from {{ ref('blur_ethereum_base_trades')}})
        and zone=0x0000000000d80cfcb8dfcd8b2c4fd9c813482938
        )
    )
on 1=1
)

select * from unit_test where diff_count > 0

