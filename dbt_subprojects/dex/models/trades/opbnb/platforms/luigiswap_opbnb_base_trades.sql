{{
    config(
        schema = 'luigiswap_opbnb',
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
        blockchain = 'opbnb',
        project = 'luigiswap',
        version = '1',
        Pair_evt_Swap = source('luigiswap_opbnb', 'uniswapv2pair_evt_swap'),
        Factory_evt_PairCreated = source('luigiswap_opbnb', 'uniswapv2factory_evt_paircreated')
    )
}}
