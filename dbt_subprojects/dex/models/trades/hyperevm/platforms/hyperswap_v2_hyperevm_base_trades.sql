{{
    config(
        schema = 'hyperswap_v2_hyperevm',
        tags = ['prod_exclude'],
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
        blockchain = 'hyperevm',
        project = 'hyperswap',
        version = '2',
        Pair_evt_Swap = source('hyperswap_hyperevm', 'hyperswappair_evt_swap'),
        Factory_evt_PairCreated = source('hyperswap_hyperevm', 'uniswapv2factory_evt_paircreated')
    )
}} 