{{ config(
    schema = 'tofu_bnb',
    alias = alias('events'),
    tags = ['dunesql'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_trade_id']
    )
}}

{{
tofu_v1_events(
    blockchain = 'bnb',
    MarketNG_call_run = source('tofu_nft_bnb', 'MarketNG_call_run'),
    MarketNG_evt_EvInventoryUpdate = source('tofu_nft_bnb', 'MarketNG_evt_EvInventoryUpdate'),
    raw_transactions = source('bnb','transactions'),
    project_start_date = "TIMESTAMP '2021-12-09'",
    NATIVE_ERC20_REPLACEMENT = '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c',
    NATIVE_SYMBOL_REPLACEMENT = 'BNB'
    )
}}
