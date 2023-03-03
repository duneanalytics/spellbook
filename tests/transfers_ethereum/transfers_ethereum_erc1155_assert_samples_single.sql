-- Bootstrapped correctness test against legacy Postgres values.
-- Also manually check etherscan info for the first 5 rows

WITH unit_tests as
(SELECT case when test_data_v1.tokenid = transfers_v2.tokenId then True else False end as test
FROM {{ ref('transfers_ethereum_erc1155') }} transfers_v2
JOIN {{ ref('transfers_ethereum_erc1155_transfersingle') }} test_data_v1 
ON test_data_v1.evt_tx_hash = transfers_v2.evt_tx_hash
AND test_data_v1.value = abs(transfers_v2.amount)
)
select count(case when test = false then 1 else null end)/count(*) as pct_mismatch, count(*) as count_rows
from unit_tests
having count(case when test = false then 1 else null end) > count(*)*0.05