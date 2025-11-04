{% set blockchain = 'worldchain' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'across_v2_withdrawals',
    materialized = 'view',
    )
}}

{{across_v2_withdrawals(
    blockchain = blockchain
    , events = source('across_v3_worldchain', 'worldchain_spokepool_evt_filledrelay')
    )}}