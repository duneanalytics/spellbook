-- Manually check against exported data from Dune https://dune.com/queries/2063220

WITH unit_tests as (
    SELECT (case when test_data.price_amount_raw = trades.amount_raw then True else False end) as price_test
    FROM {{ ref('oneplanet_polygon_events') }} trades
    JOIN {{ ref('oneplanet_polygon_trades_seed') }} test_data ON test_data.tx_hash = trades.tx_hash AND test_data.block_time = trades.block_time
    WHERE trades.block_time > '2023-01-01' and trades.block_time < '2023-01-10'
)

select count(case when price_test = false then 1 else null end)/count(*) as pct_mismatch, count(*) as count_rows
from unit_tests
having count(case when price_test = false then 1 else null end) > 0
