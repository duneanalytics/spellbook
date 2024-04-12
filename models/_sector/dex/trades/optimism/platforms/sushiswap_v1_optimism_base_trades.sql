{{
    config(
        schema = 'sushiswap_v1_optimism',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH

dexs_constant_product_pool AS (
    {{
        generic_spot_compatible_trades(
            blockchain = 'optimism',
            project = 'sushiswap',
            version = 'trident-cpp',
            source_evt_swap = source('sushi_optimism', 'ConstantProductPool_evt_Swap'),
            taker = 'recipient'
        )
    }}
),

dexs_stable_pool AS (
    {{
        generic_spot_compatible_trades(
            blockchain = 'optimism',
            project = 'sushiswap',
            version = 'trident-sp',
            source_evt_swap = source('sushi_optimism', 'StablePool_evt_Swap'),
            taker = 'recipient'
        )
    }}
)

SELECT
    dexs_cpp.blockchain,
    dexs_cpp.project,
    dexs_cpp.version,
    dexs_cpp.block_month,
    dexs_cpp.block_date,
    dexs_cpp.block_time,
    dexs_cpp.block_number,
    dexs_cpp.token_bought_amount_raw,
    dexs_cpp.token_sold_amount_raw,
    dexs_cpp.token_bought_address,
    dexs_cpp.token_sold_address,
    dexs_cpp.taker,
    dexs_cpp.maker,
    dexs_cpp.project_contract_address,
    dexs_cpp.tx_hash,
    dexs_cpp.evt_index
FROM dexs_constant_product_pool AS dexs_cpp
UNION ALL
SELECT
    dexs_sp.blockchain,
    dexs_sp.project,
    dexs_sp.version,
    dexs_sp.block_month,
    dexs_sp.block_date,
    dexs_sp.block_time,
    dexs_sp.block_number,
    dexs_sp.token_bought_amount_raw,
    dexs_sp.token_sold_amount_raw,
    dexs_sp.token_bought_address,
    dexs_sp.token_sold_address,
    dexs_sp.taker,
    dexs_sp.maker,
    dexs_sp.project_contract_address,
    dexs_sp.tx_hash,
    dexs_sp.evt_index
FROM dexs_stable_pool AS dexs_sp
