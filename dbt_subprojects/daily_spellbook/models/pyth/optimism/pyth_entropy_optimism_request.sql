{{
    config(
        schema='pyth_entropy_optimism',
        alias='request',
        materialized='incremental',
        file_format='delta',
        incremental_strategy='merge',
        unique_key=['tx_hash', 'assigned_sequence_number']
    )
}}

{{pyth_entropy_request(
    blockchain='optimism',
    symbol='ETH',
    entropy_address='0xdF21D137Aadc95588205586636710ca2890538d5'
)}}