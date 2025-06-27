{{
    config(
        schema = 'metropolis_sonic',
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
        blockchain = 'sonic',
        project = 'metropolis',
        version = '1',
        Factory_evt_function = source('metropolis_sonic', 'router_call_swapexacttokensfortokens'),
        Factory_evt_PairCreated = source('metropolis_sonic', 'v2factory_evt_paircreated')
    )
}}