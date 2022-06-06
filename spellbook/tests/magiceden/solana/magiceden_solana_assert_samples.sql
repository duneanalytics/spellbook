-- Bootstrapped correctness test against legacy Postgres values.
-- Also manually check solscan info for the first 5 rows

WITH unit_tests AS (
    SELECT COALESCE(test_data.amount = os_trades.amount, FALSE) AS price_test
    FROM {{ ref('magiceden_solana_trades') }} AS os_trades
    INNER JOIN
        {{ ref('magiceden_solana_trades_postgres') }} AS test_data ON
            test_data.tx_hash = os_trades.tx_hash
            AND test_data.block_time = os_trades.block_time
)

SELECT
    COUNT(CASE WHEN price_test = FALSE THEN 1 END) / COUNT(*) AS pct_mismatch,
    COUNT(*) AS count_rows
FROM unit_tests
HAVING COUNT(CASE WHEN price_test = FALSE THEN 1 END) > COUNT(*) * 0.1
