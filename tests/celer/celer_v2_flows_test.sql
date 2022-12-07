-- Values were manually checked on block explorers

WITH unit_tests as
(SELECT case when test_data.blockchain = cr_flows.blockchain 
                and test_data.project = cr_flows.project 
                and test_data.tx_hash = cr_flows.tx_hash 
                and test_data.sender = cr_flows.sender 
                and test_data.token_amount = cr_flows.token_amount 
then True else False end as test
FROM {{ ref('celer_v2_flows') }} cr_flows
JOIN {{ ref('bridge_flows_seed') }} test_data ON test_data.tx_hash = cr_flows.tx_hash)
select count(case when test = false then 1 else null end)/count(*) as pct_mismatch, count(*) as count_rows
from unit_tests
having count(case when test = false then 1 else null end) > count(*)*0.1