-- Bootstrapped correctness test against legacy Postgres values.
-- Postgres query: "SELECT evt_block_time, evt_tx_hash, price FROM opensea."WyvernExchange_evt_OrdersMatched" ORDER BY evt_block_time DESC LIMIT 100"
-- Also manually check etherscan info for the first 5 rows
WITH unit_tests AS (
    SELECT COALESCE(test_data.price = os_trades.amount_raw, FALSE) AS price_test
    FROM {{ ref('opensea_ethereum_trades') }} AS os_trades
    INNER JOIN
        {{ ref('opensea_ethereum_trades_postgres') }} AS test_data ON
            test_data.evt_tx_hash = os_trades.tx_hash
)

SELECT
    COUNT(CASE WHEN price_test = FALSE THEN 1 END) / COUNT(*) AS pct_mismatch,
    COUNT(*) AS count_rows
FROM unit_tests
HAVING
    COUNT(CASE WHEN price_test = FALSE THEN 1 END) > COUNT(*) * 0.05
