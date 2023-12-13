{{ config(
    
    alias = 'view_pools',
    materialized='table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "curvefi",
                                \'["yulesa", "agaperste", "ilemi"]\') }}'
    )
 }}

 ---------------------------------------------------------------- Regular Pools ----------------------------------------------------------------
WITH regular_pools AS (
    SELECT
        version,
        name,
        symbol,
        pool_address,
        token_address,
        gauge_contract,
        coin0,
        coin1,
        coin2,
        undercoin0,
        undercoin1,
        undercoin2,
        undercoin3,
        deposit_contract,
        coin3
    FROM
        {{ ref('curvefi_ethereum_pool_details') }}
),
regular_pools_deployed AS (
    SELECT
        version,
        name,
        symbol,
        pool_address,
        CAST(NULL as uint256) AS A,
        CAST(NULL as uint256) AS mid_fee,
        CAST(NULL as uint256) AS out_fee,
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
        _A,
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
        _name AS name,
        _symbol AS symbol,
        output_0 AS pool_address,
        _A AS A,
        _fee AS mid_fee,
        _fee AS out_fee,
        output_0 AS token_address,
        output_0 AS deposit_contract,
        _coins[1] AS coin0,
        _coins[2] AS coin1,
        _coins[3] AS coin2,
        _coins[4] AS coin3,
        CAST(NULL as varbinary) AS undercoin0,
        CAST(NULL as varbinary) AS undercoin1,
        CAST(NULL as varbinary) AS undercoin2,
        CAST(NULL as varbinary) AS undercoin3
    FROM
        plain_calls
),
meta_calls AS (
    SELECT
        *
    FROM (
        SELECT         
            _name,
            _symbol,
            output_0,
            call_tx_hash,
            _base_pool,
            _coin,
            _A,
            _fee 
        FROM 
        {{ source(
            'curvefi_ethereum',
            'CurveFactory_call_deploy_metapool' 
        ) }} --https://etherscan.io/address/0xb9fc157394af804a3578134a6585c0dc9cc990d4
        WHERE
        call_success

        UNION ALL 

        SELECT         
            _name,
            _symbol,
            output_0,
            call_tx_hash,
            _base_pool,
            _coin,
            _A,
            _fee 
        FROM 
        {{ source(
            'curvefi_ethereum',
            'MetaPoolFactory_call_deploy_metapool' 
        ) }} --https://etherscan.io/address/0x0959158b6040d32d04c301a72cbfd6b39e21c9ae
        WHERE
        call_success
    ) a
),
meta_pools_deployed AS (
    SELECT
        'Factory V1 Meta' AS version,
        _name AS name,
        _symbol AS symbol,
        output_0 AS pool_address,
        _A AS A,
        _fee AS mid_fee,
        _fee AS out_fee,
        output_0 AS token_address,
        output_0 AS deposit_contract,
        _coin AS coin0,
        r.token_address as coin1, --reference the token address of the base pool as coin1. meta pools swap into the base pool token, and then another swap is conducted.
        CAST(NULL as varbinary) AS coin2,
        CAST(NULL as varbinary) AS coin3,
        _coin AS undercoin0,
        --Listing underlying coins for the ExchangeUnderlying function
        r.coin0 as undercoin1,
        r.coin1 as undercoin2,
        r.coin2 as undercoin3
    FROM
        meta_calls mc 
    LEFT JOIN regular_pools r ON r.pool_address = mc._base_pool

    UNION ALL

    SELECT
        'Factory V1 Stableswap Meta' AS version,
        _name AS name,
        _symbol AS symbol,
        output_0 AS pool_address,
        _A AS A,
        _fee AS mid_fee,
        _fee AS out_fee,
        output_0 AS token_address,
        output_0 AS deposit_contract,
        _coin AS coin0,
        r.token_address as coin1, --reference the token address of the base pool as coin1. meta pools swap into the base pool token, and then another swap is conducted.
        CAST(NULL as varbinary) AS coin2,
        CAST(NULL as varbinary) AS coin3,
        _coin AS undercoin0,
        --Listing underlying coins for the ExchangeUnderlying function
        r.coin0 as undercoin1,
        r.coin1 as undercoin2,
        r.coin2 as undercoin3
    FROM
        {{ source(
            'curvefi_ethereum',
            'crvUSD_StableswapFactory_call_deploy_metapool' 
        ) }} mc 
    LEFT JOIN regular_pools r ON r.pool_address = mc._base_pool
),

v1_stableswap as (
    SELECT
        'Factory V1 Stableswap Plain' AS version,
        p._name AS name,
        p._symbol AS symbol,
        dp.pool AS pool_address,
        p._A AS A,
        p._fee AS mid_fee,
        p._fee AS out_fee,
        dp.pool AS token_address,
        dp.pool AS deposit_contract,
        p._coins[1] AS coin0,
        p._coins[2] AS coin1,
        COALESCE(try(p._coins[3]),CAST(NULL as varbinary)) as coin2,
        COALESCE(try(p._coins[4]),CAST(NULL as varbinary)) as coin3,
        CAST(NULL as varbinary) AS undercoin0,
        CAST(NULL as varbinary) AS undercoin1,
        CAST(NULL as varbinary) AS undercoin2,
        CAST(NULL as varbinary) AS undercoin3
    FROM
        {{ source(
            'curvefi_ethereum',
            'crvUSD_StableswapFactory_call_deploy_plain_pool'
        ) }} as p
        LEFT JOIN {{ source(
            'curvefi_ethereum',
            'crvUSD_StableswapFactory_evt_PlainPoolDeployed'
        ) }} dp
        ON p.call_block_time = dp.evt_block_time
        AND p.call_tx_hash = dp.evt_tx_hash
)

, v1_pools_deployed AS (
    SELECT
        version,
        name,
        symbol,
        pool_address,
        A,
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
        undercoin3
    FROM
        plain_pools_deployed
    UNION ALL
    SELECT
        version,
        name,
        symbol,
        pool_address,
        A,
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
        undercoin3
    FROM
        meta_pools_deployed
    UNION ALL
    SELECT
        version,
        name,
        symbol,
        pool_address,
        A,
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
        undercoin3
    FROM
        v1_stableswap
),
---------------------------------------------------------------- V2 Pools ----------------------------------------------------------------
v2_pools_deployed AS (
    SELECT
        'Factory V2' AS version,
        _name AS name,
        _symbol AS symbol,
        output_0 AS pool_address,
        p.A AS A,
        p.mid_fee AS mid_fee,
        p.out_fee AS out_fee,
        p.token AS token_address,
        output_0 AS deposit_contract,
        coins[1] AS coin0,
        coins[2] AS coin1,
        CAST(NULL as varbinary) AS coin2,
        CAST(NULL as varbinary) AS coin3,
        CAST(NULL as varbinary) AS undercoin0,
        CAST(NULL as varbinary) AS undercoin1,
        CAST(NULL as varbinary) AS undercoin2,
        CAST(NULL as varbinary) AS undercoin3
    FROM
        {{ source(
            'curvefi_ethereum',
            'CurveFactoryV2_evt_CryptoPoolDeployed'
        ) }} as p
        LEFT JOIN {{ source(
            'curvefi_ethereum',
            'CurveFactoryV2_call_deploy_pool'
        ) }}
        ON p.evt_block_time = call_block_time
        AND p.evt_tx_hash = call_tx_hash
),

v2_updated_pools_deployed AS (
    --still called v2 in the curve interface, but has an updated event structure to include fees and price scale
    SELECT
        'Factory V2 updated' AS version,
        p.name AS name,
        p.symbol AS symbol,
        p.pool AS pool_address,
        dp.A AS A,
        dp.mid_fee AS mid_fee,
        dp.out_fee AS out_fee,
        p.pool AS token_address,
        p.pool AS deposit_contract,
        p.coins[1] AS coin0,
        p.coins[2] AS coin1,
        COALESCE(try(p.coins[3]),CAST(NULL as varbinary)) as coin2,
        COALESCE(try(p.coins[4]),CAST(NULL as varbinary)) as coin3,
        CAST(NULL as varbinary) AS undercoin0,
        CAST(NULL as varbinary) AS undercoin1,
        CAST(NULL as varbinary) AS undercoin2,
        CAST(NULL as varbinary) AS undercoin3
    FROM
        {{ source(
            'curvefi_v2_ethereum',
            'CurveTricryptoFactory_evt_TricryptoPoolDeployed'
        ) }} as p
        LEFT JOIN {{ source(
            'curvefi_v2_ethereum',
            'CurveTricryptoFactory_call_deploy_pool'
        ) }} dp
        ON p.evt_block_time = dp.call_block_time
        AND p.evt_tx_hash = dp.call_tx_hash
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
        ) }} as g
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
        ) }} as g2
        ON pd2.pool_address = g2.token
    UNION ALL
    SELECT
        pd2u.*,
        gauge AS gauge_contract
    FROM
        v2_updated_pools_deployed pd2u
    LEFT JOIN {{ source(
        'curvefi_v2_ethereum',
        'CurveTricryptoFactory_evt_LiquidityGaugeDeployed'
        ) }} as g3
        ON pd2u.pool_address = g3.pool
),

contract_name AS (
    SELECT min(c.name) as name,
           min(c.namespace) as namespace,
           c.address
    FROM {{ source('ethereum', 'contracts') }} c
    INNER JOIN pools ON address = pool_address
    GROUP BY address
)

SELECT
    version,
    p.name,
    symbol,
    pool_address,
    CASE
        WHEN namespace IS NULL THEN 'no'
        ELSE 'yes'
    END AS decoded,
    namespace AS dune_namespace,
    C.name AS dune_table_name,
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
    array[undercoin0, undercoin1, undercoin2, undercoin3] as undercoins,
    array[coin0, coin1, coin2, coin3] as coins, --changing order to hopefully reset the CI
    gauge_contract
FROM
    pools p
LEFT JOIN contract_name C
    ON C.address = pool_address
ORDER BY
    dune_table_name DESC