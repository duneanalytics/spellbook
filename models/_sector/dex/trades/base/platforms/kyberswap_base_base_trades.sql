{{
    config(
        schema = 'kyberswap_base',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    kyberswap_base_compatible_trades(
        blockchain = 'base',
        project = 'kyberswap',
        version = '2',
        Pair_evt_Swap = source('kyber_base', 'ElasticPool_evt_Swap'),
        Factory_evt_PoolCreated = source('kyber_base', 'Factory_evt_PoolCreated')
    )
}}
