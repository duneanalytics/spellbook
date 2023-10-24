{{ config(
    schema = 'tofu_optimism',
    alias = 'events',
    
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['unique_trade_id']
    )
}}

{{
tofu_v1_events(
    blockchain = 'optimism',
    MarketNG_call_run = source('tofu_nft_optimism', 'MarketNG_call_run'),
    MarketNG_evt_EvInventoryUpdate = source('tofu_nft_optimism', 'MarketNG_evt_EvInventoryUpdate'),
    raw_transactions = source('optimism','transactions'),
    project_start_date = "TIMESTAMP '2021-12-23'",
    NATIVE_ERC20_REPLACEMENT = '0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000',
    NATIVE_SYMBOL_REPLACEMENT = 'ETH'
    )
}}
