{% set blockchain = 'linea' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'across_v3_deposits',
    materialized = 'view',
    )
}}

{{across_v3_deposits(
    blockchain = blockchain
    , events = source('across_v2_linea', 'linea_spokepool_evt_v3fundsdeposited')
    )}}