{% set blockchain = 'polygon' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'across_v2_deposits',
    materialized = 'view',
    )
}}

{{across_v2_deposits(
    blockchain = blockchain
    , events = source('across_v2_polygon', 'uba_polygon_spokepool_evt_fundsdeposited')
    )}}