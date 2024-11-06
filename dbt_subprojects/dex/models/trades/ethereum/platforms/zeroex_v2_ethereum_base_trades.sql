{{
    config(
        schema = 'zeroex_v2_ethereum',
        alias ='base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    zeroex_v2_rfq(
        blockchain = 'ethereum',
        start_date = '2024-07-15',
        version = '2'
    )
}}
