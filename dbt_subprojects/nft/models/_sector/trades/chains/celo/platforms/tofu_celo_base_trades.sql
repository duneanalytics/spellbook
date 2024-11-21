{{ config(
    schema = 'tofu_celo',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id']
    )
}}

{{
tofu_v1_base_trades(
    blockchain = 'celo',
    MarketNG_call_run = source('tofu_nft_celo', 'MarketNG_call_run'),
    MarketNG_evt_EvInventoryUpdate = source('tofu_nft_celo', 'MarketNG_evt_EvInventoryUpdate'),
    project_start_date = "TIMESTAMP '2022-01-15'",
    NATIVE_ERC20_REPLACEMENT = '0x471EcE3750Da237f93B8E339c536989b8978a438',
    NATIVE_SYMBOL_REPLACEMENT = 'CELO'
    )
}}
