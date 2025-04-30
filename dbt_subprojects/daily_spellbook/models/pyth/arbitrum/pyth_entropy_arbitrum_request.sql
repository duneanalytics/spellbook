{{
    config(
        schema='pyth_entropy_arbitrum',
        alias='request',
        materialized='incremental',
        file_format='delta',
        incremental_strategy='merge',
        unique_key=['tx_hash']
    )
}}

{{pyth_entropy_request(
    blockchain='arbitrum',
    symbol='ETH',
    entropy_address=0x7698E925FfC29655576D0b361D75Af579e20AdAc
)}}