{{
    config(
        schema = 'zonic_zksync',
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
        blockchain = 'zksync',
        min_block_number = 269129,
        project_start_date = '2023-03-27',
        c_alternative_token_address = '0x000000000000000000000000000000000000800a',
        royalty_fee_receive_address_to_skip = ['0x0000000000000000000000000000000000008001']
    )
}}
