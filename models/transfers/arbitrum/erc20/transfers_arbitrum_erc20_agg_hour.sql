{{ config(
        tags = ['dunesql'],
        schema = 'transfers_arbitrum',
        alias = alias('erc20_agg_hour'),
        materialized ='incremental',
        partition_by = ['block_month'],
        file_format ='delta',
        incremental_strategy='merge',
        unique_key = ['block_hour', 'wallet_address', 'token_address']
        )
}}

{{
    transfers_erc20_agg_hour(
        transfers_erc20 = ref('transfers_arbitrum_erc20'),
        tokens_erc20 = ref('tokens_arbitrum_erc20')
    )
}}