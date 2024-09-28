{{
    config(
        schema = 'gridex_base',
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
        blockchain = 'base',
        project = 'gridex',
        version = '1',
        Pair_evt_Swap = source('gridex_base', 'pool_evt_Swap'),
        Factory_evt_PoolCreated = source('gridex_base', 'GridFactory_evt_GridCreated'),
        taker_column_name = 'sender',
        maker_column_name = 'recipient',
        optional_columns = null,
        pair_column_name = 'grid'
    )
}}