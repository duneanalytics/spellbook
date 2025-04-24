{{
    config(
        schema = 'velodrome_unichain',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH 

dexs_v2 AS (
    {{
        uniswap_compatible_v2_trades(
            blockchain = 'unichain',
            project = 'velodrome',
            version = '2',
            Pair_evt_Swap = source('velodrome_unichain', 'Pool_evt_Swap'),
            Factory_evt_PairCreated = source('velodrome_unichain', 'PoolFactory_evt_PoolCreated'),
            pair_column_name = 'pool'
        )
    }}
),

dexs_v2_cl AS (
    {{
        uniswap_compatible_v3_trades(
            blockchain = 'unichain',
            project = 'velodrome',
            version = '2_cl',
            Pair_evt_Swap = source('velodrome_unichain', 'CLpool_evt_Swap'),
            Factory_evt_PoolCreated = source('velodrome_unichain', 'CLfactory_evt_PoolCreated'),
            optional_columns = []
        )
    }}
)

SELECT
    dexs_v2.blockchain,
    dexs_v2.project,
    dexs_v2.version,
    dexs_v2.block_month,
    dexs_v2.block_date,
    dexs_v2.block_time,
    dexs_v2.block_number,
    dexs_v2.token_bought_amount_raw,
    dexs_v2.token_sold_amount_raw,
    dexs_v2.token_bought_address,
    dexs_v2.token_sold_address,
    dexs_v2.taker,
    dexs_v2.maker,
    dexs_v2.project_contract_address,
    dexs_v2.tx_hash,
    dexs_v2.evt_index
FROM dexs_v2
UNION ALL
SELECT
    dexs_v2_cl.blockchain,
    dexs_v2_cl.project,
    dexs_v2_cl.version,
    dexs_v2_cl.block_month,
    dexs_v2_cl.block_date,
    dexs_v2_cl.block_time,
    dexs_v2_cl.block_number,
    dexs_v2_cl.token_bought_amount_raw,
    dexs_v2_cl.token_sold_amount_raw,
    dexs_v2_cl.token_bought_address,
    dexs_v2_cl.token_sold_address,
    dexs_v2_cl.taker,
    dexs_v2_cl.maker,
    dexs_v2_cl.project_contract_address,
    dexs_v2_cl.tx_hash,
    dexs_v2_cl.evt_index
FROM dexs_v2_cl
