
WITH unit_tests as
(
    SELECT case when 'WETH'= 'WETH' then True else False end test
)
select count(case when test = false then 1 else null end) as count_rows
from unit_tests
having count(case when test = false then 1 else null end) > 0

--get_chain_native_token('ethereum','prices_symbol')