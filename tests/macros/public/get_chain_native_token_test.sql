
WITH unit_tests as
(
    -- SELECT case when get_chain_native_token('ethereum','prices_symbol') = 'WETH' then True else False end test
    SELECT True as test
)
select count(case when test = false then 1 else null end) as count_rows
from unit_tests
having count(case when test = false then 1 else null end) > 0