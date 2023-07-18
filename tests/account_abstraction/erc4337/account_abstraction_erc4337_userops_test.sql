WITH unit_test AS (
    SELECT
        CASE
            WHEN test.tx_hash = actual.tx_hash THEN TRUE ELSE FALSE
        END AS tx_hash_test,
        CASE
            WHEN test.bundler = actual.bundler THEN TRUE ELSE FALSE
        END AS bundler_test

    FROM
        {{ ref('account_abstraction_erc4337_userops') }} AS actual
        INNER JOIN {{ ref('account_abstraction_erc4337_userops_seed') }} AS test
        ON   actual.userop_hash = test.userop_hash
       
)
SELECT
    *
FROM
    unit_test
WHERE
    tx_hash_test = FALSE
    OR bundler_test = FALSE
