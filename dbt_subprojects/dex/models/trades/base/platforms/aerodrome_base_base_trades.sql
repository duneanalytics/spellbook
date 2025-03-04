{{
    config(
        schema = 'aerodrome_base',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH dexs_v1 AS (
    {{
        uniswap_compatible_v2_trades(
            blockchain = 'base',
            project = 'aerodrome',
            version = '1',
            Pair_evt_Swap = source('aerodrome_base', 'Pool_evt_Swap'),
            Factory_evt_PairCreated = source('aerodrome_base', 'PoolFactory_evt_PoolCreated'),
            pair_column_name = 'pool'
    )
    }}
),

dexs_v2 AS (
    {{
        uniswap_compatible_v3_trades(
            blockchain = 'base',
            project = 'aerodrome',
            version = 'slipstream',
            Pair_evt_Swap = source('aerodrome_base', 'CLPool_evt_Swap'),
            Factory_evt_PoolCreated = source('aerodrome_base', 'CLFactory_evt_PoolCreated'),
            optional_columns = null
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

