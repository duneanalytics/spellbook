{{
    config(
        schema = 'sheriff_v4_robinhood',
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
        project = 'sheriff',
        version = '4',
        factory_address = '0x21fd9ab06cc927e66013e89b045c26b3ede7bb20',
        factory_topic = '0x91ccaa7a278130b65168c3a0c8d3bcae84cf5e43704342bd3ec0b59e59c036db',
        pool_data_word = 1,
        swap_topic = '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67',
        start_date = '2026-07-03'
    )
}}
