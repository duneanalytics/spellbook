{{
    config(
        schema = 'thena_bnb',
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
            blockchain = 'bnb',
            project = 'thena',
            version = '1',
            Pair_evt_Swap = source('thena_fi_bnb', 'pair_evt_swap'),
            Factory_evt_PairCreated = source('thena_fi_bnb', 'PairFactoryUpgradeable_evt_PairCreated')
        )
    }}
),

dexs_fusion AS (
    {{
        uniswap_compatible_v3_trades(
            blockchain = 'bnb',
            project = 'thena',
            version = 'fusion',
            Pair_evt_Swap = source('thena_bnb', 'AlgebraPool_evt_Swap'),
            Factory_evt_PoolCreated = source('thena_fi_bnb', 'PairFactoryUpgradeable_evt_Pool'),
            optional_columns = []
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
    dexs_fusion.blockchain,
    dexs_fusion.project,
    dexs_fusion.version,
    dexs_fusion.block_month,
    dexs_fusion.block_date,
    dexs_fusion.block_time,
    dexs_fusion.block_number,
    dexs_fusion.token_bought_amount_raw,
    dexs_fusion.token_sold_amount_raw,
    dexs_fusion.token_bought_address,
    dexs_fusion.token_sold_address,
    dexs_fusion.taker,
    dexs_fusion.maker,
    dexs_fusion.project_contract_address,
    dexs_fusion.tx_hash,
    dexs_fusion.evt_index
FROM dexs_fusion
