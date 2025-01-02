with unit_test1
    as (select case
                 when abs(variable_borrow_apy - 0.10808360395679557) / 0.10808360395679557 < 0.001
                 then true
                 else false
               end as test
        from   {{ ref('aave_v3_base_interest_rates' )}}
        where  reserve = 0x833589fcd6edb6e08f4c7c32d4f71b54bda02913 -- USDC
               and hour = TIMESTAMP '2025-01-02 20:00'),
    unit_test2
    as (select case
                 when abs(deposit_apy - 0.015712521763084) / 0.015712521763084 < 0.001
                 then true
                 else false
               end as test
        from   {{ ref('aave_v3_base_interest_rates' )}}
        where  symbol = 'WETH' -- 0x4200000000000000000000000000000000000006
               and hour = TIMESTAMP '2024-08-21 03:00')
select *
from   (select *
       from   unit_test1
       union
       select *
       from   unit_test2)
where  test = false 