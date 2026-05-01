{{
    config(
        schema = 'supernova_v3_ethereum',
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
        blockchain = 'ethereum',
        project = 'supernova',
        version = '3',
        Pair_evt_Swap =  source('supernova_ethereum', 'algebrapool_evt_swap'),
        Factory_evt_PoolCreated = source('supernova_ethereum', 'algebrafactory_evt_custompool'),
        optional_columns = []
    )
}}
