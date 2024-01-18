{{
    config(
        schema = 'pharaoh_avalanche_c',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH 

dexs_legacy AS (
    {{
        uniswap_compatible_v2_trades(
            blockchain = 'avalanche_c',
            project = 'pharaoh',
            version = '1',
            Pair_evt_Swap = source('pharaoh_avalanche_c', 'Pair_evt_Swap'),
            Factory_evt_PairCreated = source('pharaoh_avalanche_c', 'PairFactory_evt_PairCreated'),
            pair_column_name = 'pair'
        )
    }}
),

dexs_cl AS (
    {{
        uniswap_compatible_v3_trades(
            blockchain = 'avalanche_c',
            project = 'pharaoh',
            version = '2',
            Pair_evt_Swap = source('pharaoh_avalanche_c', 'ClPool_evt_Swap'),
            Factory_evt_PoolCreated = source('pharaoh_avalanche_c', 'ClPoolFactory_evt_PoolCreated'),
            optional_columns = null
        )
    }}
)

SELECT
    dexs_legacy.blockchain,
    dexs_legacy.project,
    dexs_legacy.version,
    dexs_legacy.block_month,
    dexs_legacy.block_date,
    dexs_legacy.block_time,
    dexs_legacy.block_number,
    dexs_legacy.token_bought_amount_raw,
    dexs_legacy.token_sold_amount_raw,
    dexs_legacy.token_bought_address,
    dexs_legacy.token_sold_address,
    dexs_legacy.taker,
    dexs_legacy.maker,
    dexs_legacy.project_contract_address,
    dexs_legacy.tx_hash,
    dexs_legacy.evt_index
FROM dexs_legacy
UNION ALL
SELECT
    dexs_cl.blockchain,
    dexs_cl.project,
    dexs_cl.version,
    dexs_cl.block_month,
    dexs_cl.block_date,
    dexs_cl.block_time,
    dexs_cl.block_number,
    dexs_cl.token_bought_amount_raw,
    dexs_cl.token_sold_amount_raw,
    dexs_cl.token_bought_address,
    dexs_cl.token_sold_address,
    dexs_cl.taker,
    dexs_cl.maker,
    dexs_cl.project_contract_address,
    dexs_cl.tx_hash,
    dexs_cl.evt_index
FROM dexs_cl
