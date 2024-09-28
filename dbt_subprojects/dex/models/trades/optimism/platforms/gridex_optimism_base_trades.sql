{{
    config(
        schema = 'gridex_optimism',
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
        blockchain = 'optimism',
        project = 'gridex',
        version = '1',
        Pair_evt_Swap = source('gridex_optimism', 'Grid_evt_Swap'),
        Factory_evt_PoolCreated = source('gridex_optimism', 'GridFactory_evt_GridCreated'),
        taker_column_name = 'sender',
        maker_column_name = 'recipient',
        optional_columns = null,
        pair_column_name = 'grid'
    )
}}