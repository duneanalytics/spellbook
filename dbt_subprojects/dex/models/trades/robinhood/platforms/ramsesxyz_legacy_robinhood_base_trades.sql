{{
    config(
        schema = 'ramsesxyz_legacy_robinhood',
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
        blockchain = 'robinhood',
        project = 'ramsesxyz',
        version = 'legacy',
        Pair_evt_Swap = source('ramsesxyz_robinhood', 'Pair_evt_Swap'),
        Factory_evt_PairCreated = source('ramsesxyz_robinhood', 'PairFactory_evt_PairCreated')
    )
}}
