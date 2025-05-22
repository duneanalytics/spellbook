{{
    config(
        schema='pyth_entropy_b3',
        alias='request',
        materialized='incremental',
        file_format='delta',
        incremental_strategy='merge',
        unique_key=['tx_hash', 'assigned_sequence_number']
    )
}}

{{pyth_entropy_request(
    blockchain='b3',
    symbol='ETH',
    entropy_address='0x5744Cbf430D99456a0A8771208b674F27f8EF0Fb'
)}}