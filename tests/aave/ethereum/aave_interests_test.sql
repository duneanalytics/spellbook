with unit_test1
    as (select case
                 when variable_borrow_apy = 0.024106036652769853 then true
                 else false
               end as test
        from   {{ ref('aave_v2_ethereum_interest_rates' )}}
        where  reserve = '0xdac17f958d2ee523a2206206994597c13d831ec7'
               and hour = '2022-08-22 12:00'),
    unit_test2
    as (select case when deposit_apy = 0.004223674732695223 then true
                 else false
               end as test
        from   {{ ref('aave_v2_ethereum_interest_rates' )}}
        where  symbol = 'USDC'
               and hour = '2022-08-25 09:00')
select *
from   (select *
       from   unit_test1
       union
       select *
       from   unit_test2)
where  test = false
