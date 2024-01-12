{{ config(
    schema = 'tofu_optimism',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}

{{
tofu_v1_base_trades(
    blockchain = 'optimism',
    MarketNG_call_run = source('tofu_nft_optimism', 'MarketNG_call_run'),
    MarketNG_evt_EvInventoryUpdate = source('tofu_nft_optimism', 'MarketNG_evt_EvInventoryUpdate'),
    project_start_date = "TIMESTAMP '2021-12-23'",
    NATIVE_ERC20_REPLACEMENT = '0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000',
    NATIVE_SYMBOL_REPLACEMENT = 'ETH'
    )
}}
