{% set blockchain = 'optimism' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'layerzero_deposits',
    materialized = 'view',
    )
}}

{{layerzero_v1_deposits(
    blockchain = blockchain
    , events = source('layerzero_' + blockchain, 'Endpoint_call_send')
    )}}

