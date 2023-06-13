with unit_test as (

select count_a, count_b, abs(count_a - count_b) as diff_count
from
    (select count(*) as count_a
     (select tx_hash
     from {{ ref('nft_events')}}
     where evt_type = 'Trade'
     union all
     select tx_hash
     from {{ ref('nft_mints')}})
     )
full outer join
    (select count(*) as count_b
    from { ref('nft_trades_unstable')}}
    )
on 1=1
)

select * from unit_test where diff_count > 0

