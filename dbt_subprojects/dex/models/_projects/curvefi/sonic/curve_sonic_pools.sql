{{ config(
    schema = 'curve_sonic',
    alias = 'pools',
    materialized = 'table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["sonic"]\',
                                "project",
                                "curve",
                                \'["benny"]\') }}'
    )
}}

---------------------------------------------------------------- Stableswap Plain Pools ----------------------------------------------------------------
WITH plain_pools AS (
    SELECT
        'Factory V1 Stableswap Plain' AS version,
        p._name AS name,
        p._symbol AS symbol,
        dp.pool AS pool_address,
        p._A AS amplification_param,
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
        {{ source('curvefi_sonic', 'curvestableswapfactory_call_deploy_plain_pool') }} as p
        LEFT JOIN {{ source('curvefi_sonic', 'curvestableswapfactory_evt_PlainPoolDeployed') }} dp
        ON p.call_block_time = dp.evt_block_time
        AND p.call_tx_hash = dp.evt_tx_hash
        AND p._coins = dp.coins
),

---------------------------------------------------------------- Stableswap Meta Pools ----------------------------------------------------------------
meta_pools AS (
    SELECT
        'Factory V1 Stableswap Meta' AS version,
        mc._name AS name,
        mc._symbol AS symbol,
        mp.pool AS pool_address,
        mc._A AS amplification_param,
        mc._fee AS mid_fee,
        mc._fee AS out_fee,
        mp.pool AS token_address,
        mp.pool AS deposit_contract,
        mc._coin AS coin0,
        r.token_address as coin1,
        CAST(NULL as varbinary) AS coin2,
        CAST(NULL as varbinary) AS coin3,
        mc._coin AS undercoin0,
        r.coin0 as undercoin1,
        r.coin1 as undercoin2,
        r.coin2 as undercoin3
    FROM
        {{ source('curvefi_sonic', 'curvestableswapfactory_call_deploy_metapool') }} mc
        LEFT JOIN {{ source('curvefi_sonic', 'curvestableswapfactory_evt_MetaPoolDeployed') }} mp
        ON mc.call_block_time = mp.evt_block_time
        AND mc.call_tx_hash = mp.evt_tx_hash
        LEFT JOIN plain_pools r ON r.pool_address = mc._base_pool
),

---------------------------------------------------------------- Combine All Pools ----------------------------------------------------------------
all_pools AS (
    SELECT
        version,
        name,
        symbol,
        pool_address,
        amplification_param,
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
        plain_pools

    UNION ALL

    SELECT
        version,
        name,
        symbol,
        pool_address,
        amplification_param,
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
        meta_pools
)

SELECT
    version,
    name,
    symbol,
    pool_address,
    'no' AS decoded,
    CAST(NULL as varchar) AS dune_namespace,
    CAST(NULL as varchar) AS dune_table_name,
    amplification_param,
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
    array[coin0, coin1, coin2, coin3] as coins,
    array[undercoin0, undercoin1, undercoin2, undercoin3] as undercoins
FROM
    all_pools
ORDER BY
    pool_address