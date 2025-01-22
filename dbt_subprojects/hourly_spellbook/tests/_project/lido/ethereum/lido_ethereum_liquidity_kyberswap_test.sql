with tests as (
select case when count(*) = 31 then true else false end AS success 
from {{ref('lido_liquidity_ethereum_kyberswap_pools')}}
where pool = 0xebfe63ba0264ad639b3c41d2bfe1ad708f683bc8
  and extract(month from time) = 3
  and extract(year from time) = 2023

)

select *
from tests
where success = false
