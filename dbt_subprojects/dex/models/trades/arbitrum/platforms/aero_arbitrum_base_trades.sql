{{
    config(
        schema = 'aero_arbitrum',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{ aero_trades(blockchain = 'arbitrum', cl_factory = '0x88a60ee77392577c355c0acb2a8d476d5a4ec03a') }}
