{% set blockchain = 'arbitrum' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'across_v2_withdrawals',
    materialized = 'view',
    )
}}

{{across_v2_withdrawals(
    blockchain = blockchain
    , events = source('across_v2_arbitrum', 'arbitrum_spokepool_evt_filledrelay')
    )}}

UNION ALL

{{across_v2_withdrawals(
    blockchain = blockchain
    , events = source('across_v2_arbitrum', 'uba_arbitrum_spokepool_evt_filledrelay')
    )}}