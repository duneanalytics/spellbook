{{
    config(
        schema='pyth_entropy_sei',
        alias='request',
        materialized='incremental',
        file_format='delta',
        incremental_strategy='merge',
        unique_key=['tx_hash', 'assigned_sequence_number']
    )
}}

{{pyth_entropy_request(
    blockchain='sei',
    symbol='SEI',
    entropy_address='0x98046Bd286715D3B0BC227Dd7a956b83D8978603'
)}}