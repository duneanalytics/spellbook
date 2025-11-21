{{
    config(
        schema = 'blackhole_v3_avalanche_c',
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
        blockchain = 'avalanche_c',
        project = 'blackhole',
        version = '3',
        Pair_evt_Swap =  source('blackhole_avalanche_c', 'algebrapool_evt_swap'),
        Factory_evt_PoolCreated = source('blackhole_avalanche_c', 'algebrafactory_evt_custompool'),
        optional_columns = []
    )
}}
