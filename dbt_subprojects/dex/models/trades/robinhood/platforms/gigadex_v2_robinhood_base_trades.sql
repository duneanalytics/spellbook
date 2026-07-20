{{
    config(
        schema = 'gigadex_v2_robinhood',
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
        project = 'gigadex',
        version = '2',
        factory_address = '0x6fdf38f92ead1adfc04b73aaa947ab254f6c0916',
        factory_topic = '0xc4805696c66d7cf352fc1d6bb633ad5ee82f6cb577c453024b6e0eb8306c6fc9',
        pool_data_word = 1,
        start_date = '2026-07-20'
    )
}}
