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

with dexs as (
    -- Constant Product Pool
    {{
        uniswap_compatible_v2_trades(
            blockchain = 'optimism',
            project = 'sushiswap',
            version = 'trident-cpp',
            Pair_evt_Swap = source('sushi_optimism', 'ConstantProductPool_evt_Swap')
        )
    }}

    union all

    -- Stable Pool
    {{
        uniswap_compatible_v2_trades(
            blockchain = 'optimism',
            project = 'sushiswap',
            version = 'trident-sp',
            Pair_evt_Swap = source('sushi_optimism', 'StablePool_evt_Swap')
        )
    }}
)

select * from dexs
