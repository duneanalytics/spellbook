{{
    config(
        schema = 'factory_c4c1d1d9_v1_robinhood',
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
        project = 'factory_c4c1d1d9',
        version = '1',
        factory_address = '0xc4c1d1d9df3c70d7f008640f6e889a25787c73b2',
        factory_topic = '0xc4805696c66d7cf352fc1d6bb633ad5ee82f6cb577c453024b6e0eb8306c6fc9',
        pool_data_word = 2,
        start_date = '2026-07-13'
    )
}}
