with tests as (
select  'Check WETH:wstETH 0.008 pool' as test_name, 
case when abs(main_token_reserve - 5430.91) < 1 then true else false end as success
from {{ref('lido_liquidity_kyberswap_ethereum')}}
where pool = lower('0xebfe63ba0264ad639b3c41d2bfe1ad708f683bc8')
  and date_trunc('day',time) = '2023-03-13'
union all

select 'Check USDC:wstETH pool' as test_name, 
case when abs(paired_token_reserve - 8056414.65) < 1 then true else false end as success
from {{ref('lido_liquidity_kyberswap_ethereum')}}
where pool = lower('0xe6bcb55f45af6a2895fadbd644ced981bfa825cb')
  and date_trunc('day',time) = '2023-02-01'
)

select *
from tests
where success is false