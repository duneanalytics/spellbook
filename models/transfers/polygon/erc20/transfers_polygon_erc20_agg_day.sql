{{ config(
        
        alias = 'erc20_agg_day',
        partition_by = ['block_month'],
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key = ['block_day', 'wallet_address', 'token_address']
        )
}}


{{
    transfers_erc20_agg_day(
        transfers_erc20 = ref('transfers_polygon_erc20'),
        tokens_erc20 = source('tokens_polygon', 'erc20')
    )
}}
