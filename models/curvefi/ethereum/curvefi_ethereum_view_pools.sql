{{ config(
    alias = 'view_pools',
    materialized='table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "curvefi",
                                \'["yulesa", "agaperste"]\') }}'
    )
 }}

{% set curvefi_ethereum_DAI_USDC_USDT_pool_contract = "0xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7" %}
{% set curvefi_ethereum_sBTC_swap_contract = "0x7fc77b5c7614e1533320ea6ddc2eb61fa00a9714" %}
{% set curvefi_ethereum_REN_swap_contract = "0x93054188d876f558f4a66b2ef1d97d16edf0895b" %}
{% set threeCRV_ethereum_token = "0x6c3f90f043a72fa612cbac8115ee7e52bde6e490" %}
{% set sbtcCRV_ethereum_token = "0x075b1bb99792c9e1041ba13afef80c91a1e70fb3" %}
{% set renCRV_ethereum_token = "0x49849c98ae39fff122806c06791fa73784fb3675" %}
{% set dai_ethereum_token = "0x6b175474e89094c44da98b954eedeac495271d0f" %}
{% set renBTC_ethereum_token = "0xeb4c2781e4eba804ce9a9803c67d0893436bb27d" %}
{% set usdc_ethereum_token = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48" %}
{% set wbtc_ethereum_token = "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599" %}
{% set usdt_ethereum_token = "0xdac17f958d2ee523a2206206994597c13d831ec7" %}
{% set sBTC_ethereum_token = "0xfe18be6b3bd88a2d2a7f928d00292e7a9963cfc6" %}
---------------------------------------------------------------- Regular Pools ----------------------------------------------------------------
WITH records AS (

    SELECT
        *
    FROM
        {{ ref('curvefi_ethereum_pool_details') }}
),
regular_pools AS (
    SELECT
        version,
        `name`,
        symbol,
        pool_address AS pool_address,
        token_address AS token_address,
        deposit_contract AS deposit_contract,
        gauge_contract AS gauge_contract,
        coin0 AS coin0,
        coin1 AS coin1,
        coin2 AS coin2,
        coin3 AS coin3,
        undercoin0 AS undercoin0,
        undercoin1 AS undercoin1,
        undercoin2 AS undercoin2,
        undercoin3 AS undercoin3
    FROM
        records
),
regular_pools_deployed AS (
    SELECT
        version,
        `name`,
        symbol,
        pool_address,
        CAST(
            NULL AS VARCHAR(5)
        ) AS A,
        CAST(
            NULL AS VARCHAR(5)
        ) AS mid_fee,
        CAST(
            NULL AS VARCHAR(5)
        ) AS out_fee,
        token_address,
        deposit_contract,
        coin0,
        coin1,
        coin2,
        coin3,
        undercoin0,
        undercoin1,
        undercoin2,
        undercoin3,
        gauge_contract
    FROM
        regular_pools
),
---------------------------------------------------------------- V1 Pools ----------------------------------------------------------------
plain_calls AS (
    SELECT
        _name,
        _symbol,
        output_0,
        _coins,
        `_A`,
        _fee
    FROM
        {{ source(
            'curvefi_ethereum',
            'CurveFactory_call_deploy_plain_pool'
        ) }}
    WHERE
        call_success
),
plain_pools_deployed AS (
    SELECT
        'Factory V1 Plain' AS version,
        _name AS `name`,
        _symbol AS symbol,
        output_0 AS pool_address,
        "_A" AS A,
        _fee AS mid_fee,
        _fee AS out_fee,
        output_0 AS token_address,
        output_0 AS deposit_contract,
        _coins [1] AS coin0,
        _coins [2] AS coin1,
        _coins [3] AS coin2,
        _coins [4] AS coin3,
        CAST(
            NULL AS VARCHAR(5)
        ) AS undercoin0,
        CAST(
            NULL AS VARCHAR(5)
        ) AS undercoin1,
        CAST(
            NULL AS VARCHAR(5)
        ) AS undercoin2,
        CAST(
            NULL AS VARCHAR(5)
        ) AS undercoin3
    FROM
        plain_calls
),
meta_calls AS (
    SELECT
        _name,
        _symbol,
        output_0,
        call_tx_hash,
        _base_pool,
        _coin,
        "_A",
        _fee
    FROM
        {{ source(
            'curvefi_ethereum',
            'CurveFactory_call_deploy_metapool'
        ) }}
    WHERE
        call_success
),
meta_pools_deployed AS (
    SELECT
        'Factory V1 Meta' AS version,
        _name AS `name`,
        _symbol AS symbol,
        output_0 AS pool_address,
        "_A" AS A,
        _fee AS mid_fee,
        _fee AS out_fee,
        output_0 AS token_address,
        output_0 AS deposit_contract,
        _coin AS coin0,
        CASE
            WHEN _base_pool = '{{curvefi_ethereum_DAI_USDC_USDT_pool_contract}}' THEN '{{threeCRV_ethereum_token}}' --changing from swap to token contract
            WHEN _base_pool = '{{curvefi_ethereum_sBTC_swap_contract}}' THEN '{{sbtcCRV_ethereum_token}}' --changing from swap to token contract
            WHEN _base_pool = '{{curvefi_ethereum_REN_swap_contract}}' THEN '{{renCRV_ethereum_token}}' --changing from swap to token contract
        END AS coin1,
        CAST(
            NULL AS VARCHAR(5)
        ) AS coin2,
        CAST(
            NULL AS VARCHAR(5)
        ) AS coin3,
        _coin AS undercoin0,
        --Listing underlying coins for the ExchangeUnderlying function
        CASE
            WHEN _base_pool = '{{curvefi_ethereum_DAI_USDC_USDT_pool_contract}}' THEN '{{DAI_ethereum_token}}'
            WHEN _base_pool = '{{curvefi_ethereum_sBTC_swap_contract}}' THEN '{{renBTC_ethereum_token}}'
            WHEN _base_pool = '{{curvefi_ethereum_REN_swap_contract}}' THEN '{{renBTC_ethereum_token}}'
        END AS undercoin1,
        CASE
            WHEN _base_pool = '{{curvefi_ethereum_DAI_USDC_USDT_pool_contract}}' THEN '{{USDC_ethereum_token}}'
            WHEN _base_pool = '{{curvefi_ethereum_sBTC_swap_contract}}' THEN '{{WBTC_ethereum_token}}'
            WHEN _base_pool = '{{curvefi_ethereum_REN_swap_contract}}' THEN '{{WBTC_ethereum_token}}'
        END AS undercoin2,
        CASE
            WHEN _base_pool = '{{curvefi_ethereum_DAI_USDC_USDT_pool_contract}}' THEN '{{USDT_ethereum_token}}'
            WHEN _base_pool = '{{curvefi_ethereum_sBTC_swap_contract}}' THEN '{{sBTC_ethereum_token}}'
        END AS undercoin3
    FROM
        meta_calls
),
v1_pools_deployed AS(
    SELECT
        *
    FROM
        plain_pools_deployed
    UNION ALL
    SELECT
        *
    FROM
        meta_pools_deployed
),
---------------------------------------------------------------- V2 Pools ----------------------------------------------------------------
v2_pools_deployed AS (
    SELECT
        'Factory V2' AS version,
        _name AS `name`,
        _symbol AS symbol,
        output_0 AS pool_address,
        p.`A` AS A,
        p.mid_fee AS mid_fee,
        p.out_fee AS out_fee,
        p.token AS token_address,
        output_0 AS deposit_contract,
        coins [1] AS coin0,
        coins [2] AS coin1,
        coins [3] AS coin2,
        coins [4] AS coin3,
        CAST(
            NULL AS VARCHAR(5)
        ) AS undercoin0,
        CAST(
            NULL AS VARCHAR(5)
        ) AS undercoin1,
        CAST(
            NULL AS VARCHAR(5)
        ) AS undercoin2,
        CAST(
            NULL AS VARCHAR(5)
        ) AS undercoin3
    FROM
        {{ source(
            'curvefi_ethereum',
            'CurveFactoryV2_evt_CryptoPoolDeployed'
        ) }}
        p
        LEFT JOIN {{ source(
            'curvefi_ethereum',
            'CurveFactoryV2_call_deploy_pool'
        ) }}
        ON p.evt_block_time = call_block_time
        AND p.evt_tx_hash = call_tx_hash
),
---------------------------------------------------------------- unioning all 3 together ----------------------------------------------------------------
pools AS (
    SELECT
        *
    FROM
        regular_pools_deployed
    UNION ALL
    SELECT
        pd.*,
        gauge AS gauge_contract
    FROM
        v1_pools_deployed pd
        LEFT JOIN {{ source(
            'curvefi_ethereum',
            'CurveFactory_evt_LiquidityGaugeDeployed'
        ) }}
        g
        ON pd.pool_address = g.pool
    UNION ALL
    SELECT
        pd2.*,
        gauge AS gauge_contract
    FROM
        v2_pools_deployed pd2
        LEFT JOIN {{ source(
            'curvefi_ethereum',
            'CurveFactoryV2_evt_LiquidityGaugeDeployed'
        ) }}
        g2
        ON pd2.pool_address = g2.token
)
SELECT
    version,
    p.`name`,
    symbol,
    pool_address,
    CASE
        WHEN namespace IS NULL THEN 'no'
        ELSE 'yes'
    END AS decoded,
    namespace AS dune_namespace,
    C.`name` AS dune_table_name,
    A AS amplification_param,
    mid_fee,
    out_fee,
    token_address,
    deposit_contract,
    coin0,
    coin1,
    coin2,
    coin3,
    undercoin0,
    undercoin1,
    undercoin2,
    undercoin3,
    gauge_contract
FROM
    pools p
    LEFT JOIN {{ source(
        'ethereum',
        'contracts'
    ) }} C
    ON C.address = pool_address
ORDER BY
    dune_table_name DESC
