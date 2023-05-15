with tests as (
select case when count(*) = 31 then true else false end AS success 
from {{ref('lido_liquidity_ethereum_kyberswap_pools')}}
where pool = lower('0xebfe63ba0264ad639b3c41d2bfe1ad708f683bc8')
  and extract('month',time) = 3
  and extract('year',time) = 2023

)

select *
from tests
where success is false