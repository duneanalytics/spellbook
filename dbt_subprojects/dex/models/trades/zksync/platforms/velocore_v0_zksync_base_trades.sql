{{
    config(
        schema = 'velocore_v0_zksync',
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
        project = 'velocore',
        version = '0',
        Pair_evt_Swap = source('velocore_v0_zksync', 'Pair_evt_Swap'),
        Factory_evt_PairCreated = source('velocore_v0_zksync', 'PairFactory_evt_PairCreated')
    )
}}
