{% set blockchain = 'worldchain' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'across_v3_deposits',
    materialized = 'view',
    )
}}

{{across_v3_deposits(
    blockchain = blockchain
    , events = source('across_v3_worldchain', 'worldchain_spokepool_evt_v3fundsdeposited')
    )}}