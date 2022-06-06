-- Bootstrapped correctness test against legacy Caladan values.

WITH unit_tests AS (
    SELECT COALESCE(
            ROUND(
                test_data.amount_raw / POWER(10, 22), 3
            ) = ROUND(token_balances.amount_raw / POWER(10, 22), 3),
            FALSE
        ) AS amount_raw_test
    FROM {{ ref('balances_ethereum_erc20_specific_wallet') }} AS test_data
    INNER JOIN
        (
            SELECT
                *
            FROM
                {{ ref('balances_ethereum_erc20_hour') }}
            WHERE wallet_address = '0xff0cefdbd6bf757cc0cc361ddfbde432186ccaa6'
        ) AS token_balances
        ON test_data.timestamp = token_balances.hour
            AND test_data.wallet_address = token_balances.wallet_address
            AND test_data.token_address = token_balances.token_address
)


SELECT
    COUNT(
        CASE WHEN amount_raw_test = FALSE THEN 1 END
    ) / COUNT(*) AS pct_mismatch,
    COUNT(*) AS count_rows
FROM unit_tests
-- Having mismatches less than 1% of rows
HAVING
    COUNT(CASE WHEN amount_raw_test = FALSE THEN 1 END) > COUNT(*) * 0.01
