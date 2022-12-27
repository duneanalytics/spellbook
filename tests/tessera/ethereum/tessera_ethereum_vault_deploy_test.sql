WITH unit_test AS (
    SELECT
        CASE
            WHEN LOWER(test.owner) = LOWER(actual.owner) THEN TRUE
            ELSE FALSE
        END AS owner_test,
        CASE
            WHEN LOWER(test.origin) = LOWER(actual.origin) THEN TRUE
            ELSE FALSE
        END AS origin_test,
        CASE
            WHEN LOWER(test.vault) = LOWER(actual.vault) THEN TRUE
            ELSE FALSE
        END AS vault_test
    FROM
        {{ ref('tessera_ethereum_vault_deploy') }} AS actual
        INNER JOIN {{ ref('tessera_ethereum_vault_deploy_test_data') }} AS test
        ON LOWER(actual.tx_hash) = LOWER(test.tx_hash)
)

SELECT
    *
FROM
    unit_test
WHERE
    owner_test = FALSE
    OR origin_test = FALSE
    OR vault_test = FALSE