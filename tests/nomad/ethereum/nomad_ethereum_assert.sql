-- Check against manually selected seed data
WITH unit_tests as
(SELECT case when test_data.original_amount_raw = nomad_transactions.original_amount_raw then True else False end as amount_test
FROM {{ ref('nomad_ethereum_view_bridge_transactions') }} nomad_transactions
JOIN {{ ref('nomad_ethereum_transactions_etherscan') }} test_data ON test_data.tx_hash = nomad_transactions.tx_hash
WHERE nomad_transactions.block_time >= '2022-07-27' and nomad_transactions.block_time < '2022-07-28'
)

select * from unit_tests where amount_test = False
