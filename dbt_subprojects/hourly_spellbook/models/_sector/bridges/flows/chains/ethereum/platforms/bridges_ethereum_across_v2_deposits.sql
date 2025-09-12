{% set blockchain = 'ethereum' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'across_v2_deposits',
    materialized = 'view',
    )
}}

{{across_v2_deposits(
    blockchain = blockchain
    , events = source('across_v2_ethereum', 'uba_ethereum_spokepool_evt_fundsdeposited')
    )}}