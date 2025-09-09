{{
    config(
        schema = 'dragon_swap_v3_sei',
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
            blockchain = 'sei',
            project = 'dragon_swap',
            version = '3',
            Pair_evt_Swap = source('dragon_swap_sei_v2_sei', 'dragonswapv2pool_evt_swap'),
            Factory_evt_PoolCreated = source('dragon_swap_sei_v2_sei', 'dragonswapv2factory_evt_poolcreated')
        )
    }}
),

deduplicated_trades AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY tx_hash, evt_index ORDER BY block_time) as rn
    FROM base_trades
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
FROM deduplicated_trades
WHERE rn = 1