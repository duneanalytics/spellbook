{{
    config(
        schema='pyth_entropy_sonic',
        alias='request',
        materialized='incremental',
        file_format='delta',
        incremental_strategy='merge',
        unique_key=['tx_hash', 'assigned_sequence_number']
    )
}}

{{pyth_entropy_request(
    blockchain='sonic',
    symbol='S',
    entropy_address='0x36825bf3fbdf5a29e2d5148bfe7dcf7b5639e320'
)}}