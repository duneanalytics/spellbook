with unit_test1
    as (select case
                 when abs(variable_borrow_apy - 0.3076100813979409) / 0.3076100813979409 < 0.001
                 then true
                 else false
               end as test
        from   {{ ref('aave_v3_base_interest_rates' )}}
        where  reserve = 0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca -- USDbC
               and hour = TIMESTAMP '2023-11-11 08:00'),
    unit_test2
    as (select case
                 when abs(deposit_apy - 0.00007259786571901644) / 0.00007259786571901644 < 0.001
                 then true
                 else false
               end as test
        from   {{ ref('aave_v3_base_interest_rates' )}}
        where  symbol = 'WETH' -- 0x4200000000000000000000000000000000000006
               and hour = TIMESTAMP '2023-09-23 07:00')
select *
from   (select *
       from   unit_test1
       union
       select *
       from   unit_test2)
where  test = false 