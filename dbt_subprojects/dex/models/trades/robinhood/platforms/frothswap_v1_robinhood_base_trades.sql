{{
    config(
        schema = 'frothswap_v1_robinhood',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    robinhood_raw_v2_compatible_trades(
        project = 'frothswap',
        version = '1',
        factory_address = '0x2b1b1fb977e1cd5f18f45571c64e373b1a73dd7f',
        factory_topic = '0xbc6e98d4b276cea82ec381a0b989171af9f2edd821714b38a24fec671cd74a1c',
        pool_data_word = 1,
        start_date = '2026-07-11'
    )
}}
