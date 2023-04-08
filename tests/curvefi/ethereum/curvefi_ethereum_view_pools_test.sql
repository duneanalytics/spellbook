-- Bootstrapped correctness test against legacy Postgres values.
WITH unit_tests AS (
    SELECT
        curvefi_view_pools.pool_address,
        curvefi_view_pools.symbol,
        curvefi_view_pools.name,
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
    --these were wrong in postgres data
    WHERE lower(curvefi_view_pools.pool_address) NOT IN (
        lower('0x43b4FdFD4Ff969587185cDB6f0BD875c5Fc83f8c')
        ,lower('0x4807862AA8b2bF68830e4C8dc86D0e9A998e085A')
        ,lower('0xd632f22692FaC7611d2AA1C0D552930D43CAEd3B')
        ,lower('0xEd279fDD11cA84bEef15AF5D39BB4d4bEE23F0cA')
        ,lower('0x5a6A4D54456819380173272A5E8E9B9904BdF41B')
        ,lower('0xecd5e75afb02efa118af914515d6521aabd189f1')
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
