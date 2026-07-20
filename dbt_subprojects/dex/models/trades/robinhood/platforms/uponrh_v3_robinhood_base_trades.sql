{{
    config(
        schema = 'uponrh_v3_robinhood',
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
        project = 'uponrh',
        version = '3',
        factory_address = '0x1ac9db4a2608ba45d6127b1737949b51bb54b7f3',
        factory_topic = '0xab0d57f0df537bb25e80245ef7748fa62353808c54d6e528a9dd20887aed9ac2',
        pool_data_word = 1,
        swap_topic = '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67',
        start_date = '2026-07-10'
    )
}}
