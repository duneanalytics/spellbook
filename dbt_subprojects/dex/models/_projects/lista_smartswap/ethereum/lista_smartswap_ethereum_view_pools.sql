{{ config(
    schema = 'lista_smartswap_ethereum',
    alias = 'view_pools',
    materialized = 'table',
    file_format = 'delta',
    filtering_columns = ['pool_address'],
    post_hook = '{{ hide_spells() }}'
) }}

WITH pools AS (
    SELECT
        '1' AS version,
        swapContract AS pool_address,
        lp AS lp_token,
        CASE
            WHEN tokenA = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
                THEN 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
            ELSE tokenA
        END AS token0,
        CASE
            WHEN tokenB = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
                THEN 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
            ELSE tokenB
        END AS token1
    FROM {{ source('lista_ethereum', 'stableswapfactory_evt_newstableswappair') }}
)

SELECT
    version,
    pool_address,
    lp_token,
    UINT256 '0' AS token_id,
    token0 AS token_address
FROM pools

UNION ALL

SELECT
    version,
    pool_address,
    lp_token,
    UINT256 '1' AS token_id,
    token1 AS token_address
FROM pools
