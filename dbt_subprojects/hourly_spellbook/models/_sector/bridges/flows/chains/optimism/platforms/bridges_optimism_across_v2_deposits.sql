{% set blockchain = 'optimism' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'across_v2_deposits',
    materialized = 'view',
    )
}}

{{across_v2_old_deposits(
    blockchain = blockchain
    , events = source('across_v2_optimism', 'uba_optimism_spokepool_evt_fundsdeposited')
    )}}

UNION ALL

{{across_v2_deposits(
    blockchain = blockchain
    , events = source('across_v2_optimism', 'optimism_spokepool_evt_fundsdeposited')
    )}}