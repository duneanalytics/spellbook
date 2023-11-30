-- Bootstrapped correctness test against legacy Caladan values.
WITH sampled_wallets AS (
    SELECT *
    FROM {{ ref('balances_base_erc20_hour') }} bal
    WHERE wallet_address IN (
            SELECT DISTINCT CAST(wallet_address AS VARBINARY)
            FROM {{ ref('balances_base_erc20_latest_entries') }}
        )
        AND bal.token_address IN (
            0x50c5725949a6f0c72e6c4a641f24049a917db0cb,
            0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca,
            0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913
        ) --'DAI', 'USDbc', 'USDC'
        AND bal.block_hour > CAST('2023-11-04' AS DATE)
        AND bal.block_hour < CAST('2023-11-06' AS DATE)
), unit_tests AS (
    SELECT
        CASE
            WHEN ROUND(CAST(test_data.amount_raw AS DOUBLE) / POWER(10, 22), 3) =
                 ROUND(CAST(token_balances.amount_raw AS DOUBLE) / POWER(10, 22), 3)
            THEN TRUE
            ELSE FALSE
        END AS amount_raw_test
    FROM {{ ref('balances_base_erc20_latest_entries') }} AS test_data
    JOIN sampled_wallets AS token_balances
    ON test_data.timestamp = token_balances.block_hour
        AND CAST(test_data.wallet_address AS VARBINARY) = CAST(token_balances.wallet_address AS VARBINARY)
        AND CAST(test_data.token_address AS VARBINARY) = CAST(token_balances.token_address AS VARBINARY)
)

SELECT
    COUNT(CASE WHEN amount_raw_test = FALSE THEN 1 ELSE NULL END) / COUNT(*) AS pct_mismatch,
    COUNT(*) AS count_rows
FROM unit_tests
-- Having mismatches less than 5% of rows
HAVING COUNT(CASE WHEN amount_raw_test = FALSE THEN 1 ELSE NULL END) > COUNT(*) * 0.05
