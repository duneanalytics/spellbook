{{
    config(
        schema = 'agni_mantle',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH pools AS (
    SELECT DISTINCT
        f.output_pool AS pool
        , token0
        , token1
    FROM {{ source('agni_mantle', 'AgniPoolDeployer_call_deploy') }} f
)

, dexs AS (
    {{
        uniswap_compatible_v3_trades(
            blockchain = 'mantle'
            , project = 'agni'
            , version = '3'
            , Pair_evt_Swap = source('agni_mantle', 'AgniPool_evt_Swap')
            , Factory_evt_PoolCreated = 'pools'
            , optional_columns = null
        )
    }}
)

SELECT *
FROM dexs