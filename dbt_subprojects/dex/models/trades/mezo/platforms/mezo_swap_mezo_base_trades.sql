{{
    config(
        schema = 'mezo_swap_mezo_base_trades',
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
            blockchain = 'mezo',
            project = 'mezo_swap',
            version = '1',
            Pair_evt_Swap = source('mezo_mezo', 'pool_evt_swap'),
            Factory_evt_PairCreated = source('mezo_mezo', 'poolfactory_evt_poolcreated'),
            pair_column_name = 'pool'
        )
    }}
),

dexs_cl AS (
    {{
        uniswap_compatible_v3_trades(
            blockchain = 'mezo',
            project = 'mezo_swap',
            version = 'cl',
            Pair_evt_Swap = source('mezo_mezo', 'clpool_evt_swap'),
            Factory_evt_PoolCreated = source('mezo_mezo', 'clfactory_evt_poolcreated'),
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
