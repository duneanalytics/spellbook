{{
    config(
        schema = 'nomiswap_bnb',
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
        project = 'nomiswap',
        version = '1',
        Pair_evt_Swap = source('nomiswap_bnb', 'NomiswapPair_evt_Swap'),
        Factory_evt_PairCreated = source('nomiswap_bnb', 'NomiswapFactory_evt_PairCreated')
    )
}}
