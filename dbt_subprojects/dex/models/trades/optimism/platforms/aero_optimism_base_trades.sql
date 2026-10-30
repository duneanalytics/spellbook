{{
    config(
        schema = 'aero_optimism',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{ aero_trades(blockchain = 'optimism', cl_factory = '0xfeab42d90055db3a0616249f76b6002d72266778') }}
