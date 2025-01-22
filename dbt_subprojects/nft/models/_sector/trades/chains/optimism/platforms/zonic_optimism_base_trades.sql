{{ config(
    schema = 'zonic_optimism',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}

{{
    zonic_base_trades(
        blockchain = 'optimism',
        min_block_number = 72260823,
        project_start_date = '2023-02-04'
    )
}}
