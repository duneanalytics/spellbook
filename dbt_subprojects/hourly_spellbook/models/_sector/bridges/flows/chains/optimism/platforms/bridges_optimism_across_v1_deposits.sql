{% set blockchain = 'optimism' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'across_v1_deposits',
    materialized = 'view',
    )
}}

{{across_v1_deposits(
    blockchain = blockchain
    , events = source('across_v2_optimism', 'optimism_spokepool_evt_fundsdeposited')
    )}}