-- Bootstrapped correctness test against legacy Postgres values.
WITH unit_tests AS (
    SELECT
        CASE
            WHEN test_data.version = curvefi_view_pools.version THEN TRUE
            ELSE FALSE
        END AS version_test,
        CASE
            WHEN test_data.symbol = curvefi_view_pools.symbol THEN TRUE
            ELSE FALSE
        END AS symbol_test,
        CASE
            WHEN test_data.name = curvefi_view_pools.name THEN TRUE
            ELSE FALSE
        END AS name_test
    FROM
        {{ ref('curvefi_ethereum_view_pools') }} AS curvefi_view_pools
        INNER JOIN {{ ref('curvefi_ethereum_view_pools_postgres') }} AS test_data
        ON LOWER(
            test_data.pool_address
        ) = LOWER(
            curvefi_view_pools.pool_address
        )
)
SELECT
    *
FROM
    unit_tests
WHERE
    version_test = FALSE
    OR symbol_test = FALSE
    OR name_test = FALSE
