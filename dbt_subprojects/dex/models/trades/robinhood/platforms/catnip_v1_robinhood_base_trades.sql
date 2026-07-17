{{
    config(
        schema = 'catnip_v1_robinhood',
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
        project = 'catnip',
        version = '1',
        factory_address = '0x002ec9782d70f4e79396c58964d4691ca648fb49',
        factory_topic = '0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9',
        pool_data_word = 1,
        start_date = '2026-07-14'
    )
}}
