-- Values were also manually checked on the Tally website https://www.tally.xyz/

WITH unit_tests as
(SELECT case when test_data.blockchain = un_proposals.blockchain
                and test_data.proposal_id = un_proposals.proposal_id
                and test_data.status = un_proposals.status
then True else False end as test
FROM {{ ref('uniswap_v3_ethereum_proposals') }} un_proposals
JOIN {{ ref('uniswap_v3_proposals_seed') }} test_data ON test_data.proposal_id = un_proposals.proposal_id
)
select count(case when test = false then 1 else null end)/count(*) as pct_mismatch, count(*) as count_rows
from unit_tests
having count(case when test = false then 1 else null end) > count(*)*0.1
