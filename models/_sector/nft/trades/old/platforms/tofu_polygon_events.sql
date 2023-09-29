{{ config(
    schema = 'tofu_polygon',
    alias = alias('events'),
    tags = ['dunesql'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'bundle_index' ]
    )
}}

{{
tofu_v1_events(
    blockchain = 'polygon',
    MarketNG_call_run = source('tofu_nft_polygon', 'MarketNG_call_run'),
    MarketNG_evt_EvInventoryUpdate = source('tofu_nft_polygon', 'MarketNG_evt_EvInventoryUpdate'),
    raw_transactions = source('polygon','transactions'),
    project_start_date = "TIMESTAMP '2021-11-01'",
    NATIVE_ERC20_REPLACEMENT = '0x0000000000000000000000000000000000001010',
    NATIVE_SYMBOL_REPLACEMENT = 'MATIC'
    )
}}
