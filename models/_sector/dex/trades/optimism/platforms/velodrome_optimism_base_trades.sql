{{
    config(
        schema = 'velodrome_optimism',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH

dexs_v1 AS (
    {{
        uniswap_compatible_v2_trades(
            blockchain = 'optimism',
            project = 'velodrome',
            version = '1',
            Pair_evt_Swap = source('velodrome_optimism', 'Pair_evt_Swap'),
            Factory_evt_PairCreated = source('velodrome_optimism', 'PairFactory_evt_PairCreated')
        )
    }}
),

dexs_v2 AS (
    {{
        uniswap_compatible_v2_trades(
            blockchain = 'optimism',
            project = 'velodrome',
            version = '2',
            Pair_evt_Swap = source('velodrome_v2_optimism', 'Pool_evt_Swap'),
            Factory_evt_PairCreated = source('velodrome_v2_optimism', 'PoolFactory_evt_PoolCreated'),
            pair_column_name = 'pool'
        )
    }}
)

SELECT
    dexs_v1.blockchain,
    dexs_v1.project,
    dexs_v1.version,
    dexs_v1.block_month,
    dexs_v1.block_date,
    dexs_v1.block_time,
    dexs_v1.block_number,
    dexs_v1.token_bought_amount_raw,
    dexs_v1.token_sold_amount_raw,
    dexs_v1.token_bought_address,
    dexs_v1.token_sold_address,
    dexs_v1.taker,
    dexs_v1.maker,
    dexs_v1.project_contract_address,
    dexs_v1.tx_hash,
    dexs_v1.evt_index
FROM dexs_v1
UNION ALL
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
