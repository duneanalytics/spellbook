{{
    config(
        schema = 'tropicalswap_mantle',
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
        blockchain = 'mantle',
        project = 'tropicalswap',
        version = '1',
        Pair_evt_Swap = source('tropicalswap_mantle', 'PancakePair_evt_Swap'),
        Factory_evt_PairCreated = source('tropicalswap_mantle', 'TropicalPair_evt_Swap')
    )
}}
