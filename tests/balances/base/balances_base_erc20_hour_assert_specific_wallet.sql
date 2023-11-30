-- Bootstrapped correctness test against legacy Caladan values.
WITH unit_tests AS (
    SELECT
        CASE
            WHEN ROUND(CAST(test_data.amount_raw AS DOUBLE) / POWER(10, 22), 3) =
                 ROUND(CAST(token_balances.amount_raw AS DOUBLE) / POWER(10, 22), 3)
            THEN TRUE
            ELSE FALSE
        END AS amount_raw_test
    FROM {{ ref('balances_base_erc20_specific_wallet') }} AS test_data
    JOIN (
        SELECT *
        FROM {{ ref('balances_base_erc20_hour') }}
        WHERE wallet_address = 0x1b72bac3772050fdcaf468cce7e20deb3cb02d89
    ) AS token_balances
    ON test_data.timestamp = token_balances.block_hour
        AND CAST(test_data.wallet_address AS VARBINARY) = token_balances.wallet_address
        AND CAST(test_data.token_address AS VARBINARY) = token_balances.token_address
)

SELECT
    AVG(CAST(amount_raw_test AS DOUBLE)) AS pct_mismatch,
    COUNT(*) AS count_rows
FROM unit_tests
-- Having mismatches less than 1% of rows
HAVING AVG(CAST(amount_raw_test AS DOUBLE)) < 0.99;
