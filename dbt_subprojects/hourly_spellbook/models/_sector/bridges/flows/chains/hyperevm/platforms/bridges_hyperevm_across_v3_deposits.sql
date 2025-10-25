{% set blockchain = 'hyperevm' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'across_v3_deposits',
    materialized = 'view',
    )
}}

{{across_v3_deposits(
    blockchain = blockchain
    , events = source('across_hyperevm', 'universal_spokepool_evt_fundsdeposited')
    )}}