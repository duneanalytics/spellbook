WITH unit_test AS (
    SELECT
        CASE
            WHEN LOWER( test.tx_hash ) = LOWER( actual.tx_hash ) THEN TRUE ELSE FALSE
        END AS tx_hash_test,
        CASE
            WHEN LOWER( test.bundler ) = LOWER( actual.bundler ) THEN TRUE ELSE FALSE
        END AS bundler_test

    FROM
        {{ ref('account_abstraction_erc4337_arbitrum_userops') }} AS actual
        INNER JOIN {{ ref('account_abstraction_erc4337_userops_seed') }} AS test
        ON LOWER(
            actual.userop_hash
        ) = LOWER(
            test.userop_hash
        )
)
SELECT
    *
FROM
    unit_test
WHERE
    tx_hash_test = FALSE
    OR bundler_test = FALSE
