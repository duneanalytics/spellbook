{{
    config(
        schema = 'shibaswap_v1_ethereum',
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
        blockchain = 'ethereum',
        project = 'shibaswap',
        version = '1',
        Pair_evt_Swap = source('shibaswap_ethereum', 'UniswapV2Pair_evt_Swap'),
        Factory_evt_PairCreated = source('shibaswap_ethereum', 'UniswapV2Factory_evt_PairCreated')
    )
}}
