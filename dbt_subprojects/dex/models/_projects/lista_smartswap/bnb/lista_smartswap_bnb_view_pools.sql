{{ config(
    schema = 'lista_smartswap_bnb',
    alias = 'view_pools',
    materialized = 'table',
    file_format = 'delta',
    post_hook = '{{ hide_spells() }}'
) }}

WITH pools AS (
    SELECT
        '1' AS version,
        swapContract AS pool_address,
        lp AS lp_token,
        CASE
            WHEN tokenA = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
                THEN 0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c
            ELSE tokenA
        END AS token0,
        CASE
            WHEN tokenB = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
                THEN 0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c
            ELSE tokenB
        END AS token1
    FROM {{ source('lista_lending_bnb', 'stableswapfactory_evt_newstableswappair') }}
    WHERE swapContract != 0xd77e86779022227226377dc30d03cf1c78439acf
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
