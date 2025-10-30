{% set blockchain = 'ethereum' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'across_v1_deposits',
    materialized = 'view',
    )
}}

{{across_v1_deposits(
    blockchain = blockchain
    , events = source('across_v2_ethereum', 'ethereum_spokepool_evt_fundsdeposited')
    )}}