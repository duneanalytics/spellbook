{{ config(
    schema = 'tofu_bnb',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id']
    )
}}

{{
tofu_v1_base_trades(
    blockchain = 'bnb',
    MarketNG_call_run = source('tofu_nft_bnb', 'MarketNG_call_run'),
    MarketNG_evt_EvInventoryUpdate = source('tofu_nft_bnb', 'MarketNG_evt_EvInventoryUpdate'),
    project_start_date = "TIMESTAMP '2021-12-09'",
    NATIVE_ERC20_REPLACEMENT = '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c',
    NATIVE_SYMBOL_REPLACEMENT = 'BNB'
    )
}}
