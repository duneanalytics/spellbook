{{
    config(
        schema='pyth_entropy_abstract',
        alias='request',
        materialized='incremental',
        file_format='delta',
        incremental_strategy='merge',
        unique_key=['tx_hash', 'assigned_sequence_number']
    )
}}

{{pyth_entropy_request_zksync_chain(
    blockchain='abstract',
    symbol='ETH',
    entropy_address='0x5a4a369F4db5df2054994AF031b7b23949b98c0e'
)}}