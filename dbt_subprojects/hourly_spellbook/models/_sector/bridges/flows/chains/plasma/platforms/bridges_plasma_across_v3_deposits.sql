{% set blockchain = 'plasma' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'across_v3_deposits',
    materialized = 'view',
    )
}}

{{across_v3_deposits(
    blockchain = blockchain
    , events = source('across_plasma', 'universal_spokepool_evt_fundsdeposited')
    )}}