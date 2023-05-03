with unit_test as (

select count_a, count_b, (count_a - count_b) as diff_count
from
    (select count(*) as count_a
     from {{ ref('foundation_ethereum_base_trades')}}
     )
full outer join
    (select sum(count_b) as count_b
    from (
        select count(*) as count_b from {{ source('foundation_ethereum','market_evt_ReserveAuctionFinalized') }}
        union all
        select count(*) as count_b from {{ source('foundation_ethereum','market_evt_BuyPriceAccepted') }}
        union all
        select count(*) as count_b from {{ source('foundation_ethereum','market_evt_OfferAccepted') }}
        union all
        select count(*) as count_b from {{ source('foundation_ethereum','market_evt_PrivateSaleFinalized') }}
        )
    )
on 1=1
)

select * from unit_test where diff_count > 0

