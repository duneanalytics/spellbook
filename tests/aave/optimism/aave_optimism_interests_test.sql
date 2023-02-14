with unit_test1
    as (select case
                 when variable_borrow_apy = 0.03485010188503056 then true
                 else false
               end as test
        from   {{ ref('aave_v3_optimism_interest_rates' )}}
        where  reserve = '0x7f5c764cbc14f9669b88837ca1490cca17c31607'
        
               and hour = '2022-09-11 03:00'),
    unit_test2
    as (select case when deposit_apy = 0.027884763191936474 then true
                 else false
               end as test
        from   {{ ref('aave_v3_optimism_interest_rates' )}}
        where  symbol = 'sUSD'

               and hour = '2022-08-25 09:00')
select *
from   (select *
       from   unit_test1
       union
       select *
       from   unit_test2)
where  test = false
