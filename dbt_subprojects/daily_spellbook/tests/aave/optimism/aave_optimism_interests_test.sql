with
unit_test1 as (
    select case
               when abs(variable_borrow_apy - 0.03485010188503055) / 03485010188503055 < 0.001
               then true
               else false
            end as test
    from {{ ref('aave_v3_optimism_interest_rates' )}}
    where reserve = 0x7f5c764cbc14f9669b88837ca1490cca17c31607
      and hour = TIMESTAMP '2022-09-11 03:00'
),
unit_test2 as (
    select case
               when abs(deposit_apy - 0.02788476319193648) / 02788476319193648 < 0.001
               then true
               else false
               end as test
    from {{ ref('aave_v3_optimism_interest_rates' )}}
    where symbol = 'sUSD'
        and hour = TIMESTAMP '2022-08-25 09:00'
)
select *
from (select *
      from unit_test1
      union
      select *
      from unit_test2)
where  test = false
