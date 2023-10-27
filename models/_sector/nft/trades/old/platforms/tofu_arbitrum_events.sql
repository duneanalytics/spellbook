{{ config(
    schema = 'tofu_arbitrum',
    alias = 'events',
    
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'bundle_index' ]
    )
}}

{{
tofu_v1_events(
    blockchain = 'arbitrum',
    MarketNG_call_run = source('tofunft_arbitrum', 'MarketNG_call_run'),
    MarketNG_evt_EvInventoryUpdate = source('tofunft_arbitrum', 'MarketNG_evt_EvInventoryUpdate'),
    raw_transactions = source('arbitrum','transactions'),
    project_start_date = "TIMESTAMP '2021-12-09'",
    NATIVE_ERC20_REPLACEMENT = '0x82af49447d8a07e3bd95bd0d56f35241523fbab1',
    NATIVE_SYMBOL_REPLACEMENT = 'ARETH'
    )
}}
