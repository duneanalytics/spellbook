{{ config(
        
        alias = 'erc20_agg_hour',
        partition_by = ['block_month'],
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key = ['block_hour', 'wallet_address', 'token_address']
        )
}}

{{
    transfers_erc20_agg_hour(
        transfers_erc20 = ref('transfers_ethereum_erc20'),
        tokens_erc20 = ref('tokens_ethereum_erc20')
    )
}}
