{{
    config(
        schema = 'pancakeswap_v3_robinhood',
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
        project = 'pancakeswap',
        version = '3',
        factory_address = '0x0bfbcf9fa4f9c56b0f40a671ad40e0805a091865',
        factory_topic = '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118',
        pool_data_word = 2,
        swap_topic = '0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83',
        start_date = '2026-06-30'
    )
}}
