{{
    config(
        schema='pyth_entropy_mode',
        alias='request',
        materialized='incremental',
        file_format='delta',
        incremental_strategy='merge',
        unique_key=['tx_hash', 'assigned_sequence_number']
    )
}}

{{  pyth_entropy_request(
    blockchain='mode',
    symbol='ETH',
    entropy_address='0x8D254a21b3C86D32F7179855531CE99164721933'
)}}