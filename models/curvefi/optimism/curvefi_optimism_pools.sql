{{ config(
    schema = 'curvefi_optimism',
    alias = 'pools',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tokenid', 'token', 'pool'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "curvefi",
                                \'["msilb7"]\') }}'
    )
}}

-- Original Ref - Dune v1 Abstraction: https://github.com/duneanalytics/spellbook/blob/main/deprecated-dune-v1-abstractions/optimism2/dex/insert_curve.sql

WITH base_pools AS (
    SELECT `arg0` AS tokenid, output_0 AS token, contract_address AS pool
        FROM {{ source('curvefi_optimism', 'StableSwap_call_coins') }}
        WHERE call_success
    GROUP BY 1,2,3 --unique
    )
, meta_pools AS (
SELECT tokenid, token, et.`contract_address` AS pool
FROM (
    SELECT mp.evt_tx_hash, bp.tokenid + 1 AS tokenid, bp.token, mp.evt_block_number
    FROM {{ source('curvefi_optimism', 'PoolFactory_evt_MetaPoolDeployed') }} mp
    INNER JOIN base_pools bp
        ON mp.base_pool = bp.pool

    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
    
GROUP BY 1,2,3,4 --unique
    
    UNION ALL
    SELECT mp.evt_tx_hash, 0 AS tokenid, mp.`coin` AS token, mp.evt_block_number
    FROM {{ source('curvefi_optimism', 'PoolFactory_evt_MetaPoolDeployed') }} mp
    INNER JOIN base_pools bp
        ON mp.base_pool = bp.pool
    
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
    
GROUP BY 1,2,3,4 --unique
    
    ) mps
    -- the exchange address appears as an erc20 minted to itself (not in the deploymeny event)
    INNER JOIN {{ source('erc20_optimism','evt_Transfer') }} et
        ON et.evt_tx_hash = mps.evt_tx_hash
        AND et.`from` = '0x0000000000000000000000000000000000000000'
        AND et.`to` = et.`contract_address`
        AND et.evt_block_number = mps.evt_block_number
        
        
    GROUP BY 1,2,3
)

, basic_pools AS (
SELECT pos AS tokenid, col AS token, pool
    FROM (
        SELECT 
        posexplode(_coins), output_0 AS pool
        FROM {{ source('curvefi_optimism', 'PoolFactory_call_deploy_plain_pool') }}
        WHERE call_success
        {% if is_incremental() %}
        AND call_block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
        ) a
    GROUP BY 1,2,3
)


SELECT version, tokenid, token, pool FROM (
    SELECT 'Base Pool' AS version, tokenid, token, pool FROM base_pools
    UNION ALL
    SELECT 'Meta Pool' AS version, tokenid, token, pool FROM meta_pools
    UNION ALL
    SELECT 'Basic Pool' AS version, tokenid, token, pool FROM basic_pools
    ) a
GROUP BY 1,2,3,4 --unique
-- ORDER BY pool ASC, tokenid ASC