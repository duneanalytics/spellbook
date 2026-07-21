{{
    config(
        schema = 'aeon_protocol_v1_robinhood',
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
        blockchain = 'robinhood',
        project = 'aeon_protocol',
        version = '1',
        Pair_evt_Swap = source('aeon_robinhood', 'AeonPoolRH_evt_Swap'),
        Factory_evt_PoolCreated = source('aeon_robinhood', 'AeonFactoryRH_evt_PoolCreated'),
        optional_columns = []
    )
}}
