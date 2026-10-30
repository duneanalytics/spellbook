{{
    config(
        schema = 'aero_base',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{ aero_trades(blockchain = 'base', cl_factory = '0x9cb220581b91b0c83223012fe69688b92ad07a76') }}
