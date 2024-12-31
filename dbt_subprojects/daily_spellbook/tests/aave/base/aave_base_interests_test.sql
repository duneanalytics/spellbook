with unit_test1
    as (select case
                 when abs(variable_borrow_apy - 0.03485010188503055) / 0.03485010188503055 < 0.001
                 then true
                 else false
               end as test
        from   {{ ref('aave_v3_base_interest_rates' )}}
        where  reserve = 0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913
               and hour = TIMESTAMP '2024-12-31 08:00'),
    unit_test2
    as (select case
                 when abs(deposit_apy - 0.02788476319193648) / 0.02788476319193648 < 0.001
                 then true
                 else false
               end as test
        from   {{ ref('aave_v3_base_interest_rates' )}}
        where  symbol = 'USDC'
               and hour = TIMESTAMP '2024-12-24 05:00')
select *
from   (select *
       from   unit_test1
       union
       select *
       from   unit_test2)
where  test = false 