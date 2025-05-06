{{
    config(
        schema='pyth_entropy_apechain',
        alias='request',
        materialized='incremental',
        file_format='delta',
        incremental_strategy='merge',
        unique_key=['tx_hash', 'assigned_sequence_number']
    )
}}

{{pyth_entropy_request(
    blockchain='apechain',
    symbol='APE',
    entropy_address='0x36825bf3Fbdf5a29E2d5148bfe7Dcf7B5639e320'
)}}