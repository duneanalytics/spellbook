{{
    config(
        schema = 'factory_46c695d1_v3_robinhood',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    robinhood_raw_v3_compatible_trades(
        project = 'factory_46c695d1',
        version = '3',
        factory_address = '0x46c695d1eba2411824c22df59b8f488493a72aa5',
        factory_topic = '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118',
        pool_data_word = 2,
        swap_topic = '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67',
        start_date = '2026-07-13'
    )
}}
