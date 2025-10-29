{% set blockchain = 'optimism' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'across_v2_withdrawals',
    materialized = 'view',
    )
}}

{{across_v2_withdrawals(
    blockchain = blockchain
    , events = source('across_v2_optimism', 'optimism_spokepool_evt_filledrelay')
    )}}

UNION ALL

{{across_v2_withdrawals(
    blockchain = blockchain
    , events = source('across_v2_optimism', 'uba_optimism_spokepool_evt_filledrelay')
    )}}