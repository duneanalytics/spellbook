{{
    config(
        schema = 'zeroex_settler_worldchain',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    zeroex_settler_rfq(
        blockchain = 'worldchain'
    )
}}
