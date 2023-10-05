with unit_test as (

select count_a, count_b, abs(count_a - count_b) as diff_count
from
    (select count(*) as count_a
     from {{ ref('zora_v1_ethereum_base_trades')}}
     )
full outer join
    (select count(*) as count_b
    from {{ source('zora_ethereum','Market_evt_BidFinalized') }}
    where from_hex(json_extract_scalar(bid, '$.bidder')) != 0xe468ce99444174bd3bbbed09209577d25d1ad673
    and evt_block_time <= (select max(block_time) from {{ ref('zora_v1_ethereum_base_trades')}})
    )
on 1=1
)

select * from unit_test where diff_count > 0

