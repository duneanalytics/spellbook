-- Check that the accounting table provided a 0 net balance for 2022 and the correct amount
with balance_2022 as (
  select sum(case when left(primary_label, 1) = '1' then value_usd else -value_usd end) as balance,
         sum(abs(value_usd))                                               as abs_balance
  from {{ ref('lido_ethereum_accounting') }} --?? как у нас будет называться схема
  where extract(year from period) = 2022
),
tests as (
  select 
    'Check balance is 0 for 2022' as test_name, 
    case when abs(balance) < 0.01 then true else false end as success
  from balance_2022
  union all
  select 
    'Check abs sum of value is 17,781,439,848.176 for 2022' as test_name, 
    case when abs(abs_balance - 17781439848.176) < 1 then true else false end as success
  from balance_2022
)
select *
from tests
where success is false