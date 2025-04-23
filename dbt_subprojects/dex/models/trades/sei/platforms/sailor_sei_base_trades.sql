{{
    config(
        schema = 'sailor_sei',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    uniswap_compatible_v3_trades(
        blockchain = 'sei',
        project = 'sailor',
        version = '3',
        Pair_evt_Swap = source('sailor_sei', 'SailorV3Pool_evt_Swap'),
        Factory_evt_PairCreated = source('sailor_sei', 'SailorV3Factory_evt_PoolCreated')
    )
}}