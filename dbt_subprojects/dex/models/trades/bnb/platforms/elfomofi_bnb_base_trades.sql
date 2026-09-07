{{
    config(
        schema = 'elfomofi_bnb',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        filtering_columns = ['block_time']
    )
}}

{{
    elfomofi_compatible_trades(
        blockchain = 'bnb',
        logs = source('bnb', 'logs'),
        start_date = '2026-03-03'
    )
}}
