{{ config(
    schema = 'tofu_polygon',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}

{{
tofu_v1_base_trades(
    blockchain = 'polygon',
    MarketNG_call_run = source('tofu_nft_polygon', 'MarketNG_call_run'),
    MarketNG_evt_EvInventoryUpdate = source('tofu_nft_polygon', 'MarketNG_evt_EvInventoryUpdate'),
    project_start_date = "TIMESTAMP '2021-11-01'",
    NATIVE_ERC20_REPLACEMENT = '0x0000000000000000000000000000000000001010',
    NATIVE_SYMBOL_REPLACEMENT = 'MATIC'
    )
}}
