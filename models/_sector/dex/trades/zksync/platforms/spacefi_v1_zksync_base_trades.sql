{{
    config(
        schema = 'spacefi_v1_zksync',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    uniswap_compatible_v2_trades(
        blockchain = 'zksync',
        project = 'spacefi',
        version = '1',
        Pair_evt_Swap = source('spacefi_v1_zksync', 'UniswapV2Pool_evt_Swap'),
        Factory_evt_PairCreated = source('spacefi_v1_zksync', 'SwapFactory_evt_PairCreated')
    )
}}
