{{ config(
    schema = 'mummy_finance_optimism'
    ,alias = 'base_trades'
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,incremental_strategy = 'merge'
    ,unique_key = ['tx_hash', 'evt_index']
    ,incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    generic_spot_compatible_trades(
        blockchain = 'optimism',
        project = 'mummy_finance',
        version = '1',
        source_evt_swap = source('mummy_finance_optimism', 'Router_evt_Swap')
    )
}}
