{{ config(
        tags = ['dunesql'],
        schema = 'transfers_ethereum_eth',
        alias = alias('eth_agg_day'),
        partition_by = ['block_month'],
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key = ['block_day', 'wallet_address', 'token_address']
        )
}}

{{
    transfers_fungible_agg_day(
        transfers_native = ref('transfers_ethereum_eth_tfers'),
        native_token_symbol = 'ETH'
    )
}}