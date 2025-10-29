{% set blockchain = 'polygon' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'across_v2_withdrawals',
    materialized = 'view',
    )
}}

{{across_v2_withdrawals(
    blockchain = blockchain
    , events = source('across_v2_polygon', 'polygon_spokepool_evt_filledrelay')
    )}}

UNION ALL

{{across_v2_withdrawals(
    blockchain = blockchain
    , events = source('across_v2_polygon', 'uba_polygon_spokepool_evt_filledrelay')
    )}}