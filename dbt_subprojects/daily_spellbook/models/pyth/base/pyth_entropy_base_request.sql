{{
    config(
        schema='pyth_entropy_base',
        alias='request',
        materialized='incremental',
        file_format='delta',
        incremental_strategy='merge',
        unique_key=['tx_hash', 'assigned_sequence_number']
    )
}}

{{pyth_entropy_request(
    blockchain='base',
    symbol='ETH',
    entropy_address='0x6E7D74FA7d5c90FEF9F0512987605a6d546181Bb'
)}}