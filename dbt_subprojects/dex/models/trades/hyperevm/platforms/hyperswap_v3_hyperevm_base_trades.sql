{{
    config(
        schema = 'hyperswap_v3_hyperevm',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH base_trades AS (
    {{
        uniswap_compatible_v3_trades(
            blockchain = 'hyperevm',
            project = 'hyperswap',
            version = '3',
            Pair_evt_Swap = source('hyperswap_hyperevm', 'hyperswapv3pool_evt_swap'),
            Factory_evt_PoolCreated = source('hyperswap_hyperevm', 'hyperswapv3factory_evt_poolcreated')
        )
    }}
)

-- row_number() dedup removed: source tables verified duplicate-free via Dune queries 6935096-6935100, 6935776 (2026-04-01)
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
FROM base_trades