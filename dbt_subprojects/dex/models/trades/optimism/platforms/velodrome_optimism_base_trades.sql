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
),

dexs_v2_cl AS (
    {{
        uniswap_compatible_v3_trades(
            blockchain = 'optimism',
            project = 'velodrome',
            version = '2_cl',
            Pair_evt_Swap = source('velodrome_v2_optimism', 'CLPool_evt_Swap'),
            Factory_evt_PoolCreated = source('velodrome_v2_optimism', 'CLFactory_evt_PoolCreated'),
            optional_columns = []
        )
    }}
)

SELECT
    blockchain,
    project,
    version,
    block_month,
    block_date,
    block_time,
    block_number,
    token_bought_amount_raw,
    token_sold_amount_raw,
    token_bought_address,
    token_sold_address,
    taker,
    maker,
    project_contract_address,
    tx_hash,
    evt_index
FROM dexs_v1

UNION ALL

SELECT
    blockchain,
    project,
    version,
    block_month,
    block_date,
    block_time,
    block_number,
    token_bought_amount_raw,
    token_sold_amount_raw,
    token_bought_address,
    token_sold_address,
    taker,
    maker,
    project_contract_address,
    tx_hash,
    evt_index
FROM dexs_v2

UNION ALL

SELECT
    blockchain,
    project,
    version,
    block_month,
    block_date,
    block_time,
    block_number,
    token_bought_amount_raw,
    token_sold_amount_raw,
    token_bought_address,
    token_sold_address,
    taker,
    maker,
    project_contract_address,
    tx_hash,
    evt_index
FROM dexs_v2_cl
