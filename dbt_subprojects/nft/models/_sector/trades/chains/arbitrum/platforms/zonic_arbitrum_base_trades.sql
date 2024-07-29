{{
    config(
        schema = 'zonic_arbitrum',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    )
}}

{{
    zonic_base_trades(
        blockchain = 'arbitrum',
        min_block_number = 57932986,
        project_start_date = '2023-02-04',
        c_alternative_token_address = '0x82aF49447D8a07e3bd95BD0d56f35241523fBab1'
    )
}}
