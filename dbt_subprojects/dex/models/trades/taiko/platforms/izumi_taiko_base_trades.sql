{{
    config(
        schema = 'izumi_taiko',
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
        blockchain = 'taiko',
        project = 'izumi',
        version = '1',
        Pair_evt_Swap = source('izumi_taiko', 'iZiSwapPool_evt_Swap'),
        Factory_evt_PoolCreated = source('izumi_taiko', 'iZiSwapFactory_evt_NewPool')
    )
}}
