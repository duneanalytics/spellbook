
WITH unit_tests as
(
    SELECT case when get_href('https://etherscan.io/', 'etherscan') = concat('<a href="', 'https://etherscan.io/', '"target ="_blank">', 'etherscan') then True
     else False end test
)
select count(case when test = false then 1 else null end) as count_rows
from unit_tests
having count(case when test = false then 1 else null end) > 0