{{ config(
    schema = 'tofu_arbitrum',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id']
    )
}}

{{
tofu_v1_base_trades(
    blockchain = 'arbitrum',
    MarketNG_call_run = source('tofunft_arbitrum', 'MarketNG_call_run'),
    MarketNG_evt_EvInventoryUpdate = source('tofunft_arbitrum', 'MarketNG_evt_EvInventoryUpdate'),
    project_start_date = "TIMESTAMP '2021-12-09'",
    NATIVE_ERC20_REPLACEMENT = '0x82af49447d8a07e3bd95bd0d56f35241523fbab1',
    NATIVE_SYMBOL_REPLACEMENT = 'ARETH'
    )
}}
