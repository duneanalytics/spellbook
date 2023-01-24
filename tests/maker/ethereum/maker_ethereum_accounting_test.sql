-- Check that the accounting table provided a 0 net balance for 2022 and the correct amount
with balance_2022 as (
  select sum(case when left(code, 1) = '1' then value else -value end) as balance,
         sum(abs(value))                                               as abs_balance
  from {{ ref('maker_ethereum_accounting') }}
  where extract(year from ts) = 2022
),
tests as (
  select 
    'Check balance is 0 for 2022' as test_name, 
    case when abs(balance) < 0.01 then true else false end as success
  from balance_2022
  union all
  select 
    'Check abs sum of value is 137,785,982,132.94 for 2022' as test_name, 
    case when abs(abs_balance - 137785982132.94) < 1 then true else false end as success
  from balance_2022
)
select *
from tests
where success is false