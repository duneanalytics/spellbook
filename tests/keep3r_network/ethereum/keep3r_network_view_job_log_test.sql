WITH unit_tests AS (
    SELECT
        CASE
            WHEN test.amount = ROUND(
                actual.amount,
                2
            ) THEN TRUE
            ELSE FALSE
        END AS amount_test,
        CASE
            WHEN LOWER(
                test.job
            ) = LOWER(
                actual.job
            ) THEN TRUE
            ELSE FALSE
        END AS job_test,
        CASE
            WHEN LOWER(
                test.keeper
            ) = LOWER(
                actual.keeper
            ) THEN TRUE
            ELSE FALSE
        END AS keeper_test,
        CASE
            WHEN LOWER(
                test.token
            ) = LOWER(
                actual.token
            ) THEN TRUE
            ELSE FALSE
        END AS token_test
    FROM
        {{ ref('keep3r_network_ethereum_view_job_log') }} AS actual
        INNER JOIN {{ ref('keep3r_network_ethereum_view_job_log_postgres') }} AS test
        ON LOWER(
            test.tx_hash
        ) = LOWER(
            actual.tx_hash
        )
)
SELECT
    *
FROM
    unit_tests
WHERE
    amount_test = FALSE
    OR job_test = FALSE
    OR keeper_test = FALSE
    OR token_test = FALSE
