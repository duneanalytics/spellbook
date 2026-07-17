{{
    config(
        schema = 'aeon_protocol_v1_robinhood',
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
        project = 'aeon_protocol',
        version = '1',
        factory_address = '0xe27ea15df9e69ce06ab8ee5a2029bd699f9cf9fc',
        factory_topic = '0x97d23878912fda7f82bd5a8502f2c6e9086f5a4728f9054627788b3162f86ced',
        pool_data_word = 2,
        start_date = '2026-07-09'
    )
}}
