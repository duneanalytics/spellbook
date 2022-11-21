-- Values were also manually checked on the Tally website https://www.tally.xyz/

WITH unit_tests as
(SELECT case when test_data.blockchain = un_votes.blockchain 
                and test_data.proposal_id = un_votes.proposal_id 
                and test_data.voter_address = un_votes.voter_address 
                and round(test_data.votes/1e6,1) = round(un_votes.votes/1e6,1)
then True else False end as test
FROM {{ ref('uniswap_v3_ethereum_votes') }} un_votes
JOIN {{ ref('uniswap_v3_votes_test') }} test_data ON test_data.proposal_id = un_votes.proposal_id
AND test_data.voter_address = un_votes.voter_address
)
select count(case when test = false then 1 else null end)/count(*) as pct_mismatch, count(*) as count_rows
from unit_tests
having count(case when test = false then 1 else null end) > count(*)*0.1


