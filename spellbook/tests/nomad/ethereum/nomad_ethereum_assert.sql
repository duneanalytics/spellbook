-- Bootstrapped correctness test against legacy Postgres values.
-- Also manually check etherscan info for the first 5 rows
WITH unit_tests as
(SELECT case when test_data.original_amount_raw = nomad_transactions.original_amount_raw then True else False end as amount_test
FROM {{ ref('nomad_ethereum_view_bridge_transactions') }} nomad_transactions
JOIN {{ ref('nomad_ethereum_transactions_etherscan') }} test_data ON test_data.tx_hash = nomad_transactions.tx_hash
WHERE nomad_transactions.block_time >= '2022-07-27' and nomad_transactions.block_time < '2022-07-28'
)
select count(case when amount_test = false then 1 else null end)/count(*) as pct_mismatch, count(*) as count_rows
from unit_tests
having count(case when amount_test = false then 1 else null end) > count(*)*0.05
