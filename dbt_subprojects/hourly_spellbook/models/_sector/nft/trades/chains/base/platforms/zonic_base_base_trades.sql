{{
    config(
        schema = 'zonic_base',
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
        blockchain = 'base',
        min_block_number = 2882386,
        project_start_date = '2023-08-20',
        c_alternative_token_address = '0x4200000000000000000000000000000000000006'
    )
}}
