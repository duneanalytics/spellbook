{{
    config(
        schema = 'biswap_v2_bnb',
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
        blockchain = 'bnb',
        project = 'biswap',
        version = '2',
        Pair_evt_Swap = source('biswap_bnb', 'BiswapPair_evt_Swap'),
        Factory_evt_PairCreated = source('biswap_bnb', 'BiswapFactory_evt_PairCreated')
    )
}}
