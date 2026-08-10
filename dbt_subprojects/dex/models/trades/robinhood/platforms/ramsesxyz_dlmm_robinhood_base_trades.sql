{{
    config(
        schema = 'ramsesxyz_dlmm_robinhood',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    trader_joe_compatible_v2_1_trades(
        blockchain = 'robinhood',
        project = 'ramsesxyz',
        version = 'dlmm',
        Pair_evt_Swap = source('ramsesxyz_robinhood', 'DLMMPool_evt_Swap'),
        Factory_evt_PoolCreated = source('ramsesxyz_robinhood', 'DLMMFactory_evt_LBPairCreated'),
        pair_column_name = 'lbpair'
    )
}}
