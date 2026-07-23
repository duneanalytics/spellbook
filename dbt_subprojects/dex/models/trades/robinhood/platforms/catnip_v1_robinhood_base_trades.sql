{{
    config(
        schema = 'catnip_v1_robinhood',
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
        project = 'catnip',
        version = '1',
        Pair_evt_Swap = source('catnip_robinhood', 'AlleyPair_evt_Swap'),
        Factory_evt_PairCreated = source('catnip_robinhood', 'AlleyFactory_evt_PairCreated')
    )
}}
