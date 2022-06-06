-- Bootstrapped correctness test against legacy Postgres values.
-- Postgres query: "SELECT block_time, tx_hash, token_a_amount, token_b_amount FROM dex.trades 
-- WHERE project = 'Uniswap' AND version = '2' 
-- ORDER BY block_time DESC LIMIT 1000"

-- Also manually check etherscan info for the first 5 rows
WITH unit_tests AS (
    SELECT COALESCE(
            test_data.token_a_amount = us_trades.token_a_amount AND test_data.token_b_amount = us_trades.token_b_amount,
            FALSE
        ) AS price_test
    FROM {{ ref('uniswap_v2_ethereum_trades') }} AS us_trades
    INNER JOIN
        {{ ref('uniswap_v2_ethereum_trades_postgres') }} AS test_data ON
            test_data.tx_hash = us_trades.tx_hash
)

SELECT
    COUNT(CASE WHEN price_test = FALSE THEN 1 END) / COUNT(*) AS pct_mismatch,
    COUNT(*) AS count_rows
FROM unit_tests
HAVING
    COUNT(CASE WHEN price_test = FALSE THEN 1 END) > COUNT(*) * 0.05
