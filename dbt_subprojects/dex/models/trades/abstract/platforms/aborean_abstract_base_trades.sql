{{
    config(
        schema = 'aborean_abstract',
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
        blockchain = 'abstract',
        project = 'aborean',
        version = '3',
        Pair_evt_Swap = source('aborean_abstract', 'pool_evt_swap'),
        Factory_evt_PairCreated = source('aborean_abstract', 'poolfactory_evt_poolcreated')
        , pair_column_name = 'pool'
    )
}}