{{
    config(
        schema = 'etherex_v3_linea',
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
        blockchain = 'linea',
        project = 'etherex',
        version = '3',
        Pair_evt_Swap = source('etherex_linea', 'ramsesv3pool_evt_swap'),
        Factory_evt_PoolCreated = source('etherex_linea', 'EtherexV3Factory_evt_PoolCreated')
    )
}}