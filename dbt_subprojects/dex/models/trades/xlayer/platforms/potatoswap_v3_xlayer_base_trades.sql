{{
    config(
        schema = 'potatoswap_v3_xlayer',
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
        blockchain = 'xlayer',
        project = 'potatoswap',
        version = '3',
        Pair_evt_Swap = source('potatoswap_xlayer', 'HyperindexV3Pool_evt_Swap'),
        Factory_evt_PoolCreated = source('potatoswap_xlayer', 'HyperindexV3Factory_evt_PoolCreated')
    )
}}

