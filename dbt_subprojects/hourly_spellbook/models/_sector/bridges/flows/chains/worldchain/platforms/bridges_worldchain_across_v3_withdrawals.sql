{% set blockchain = 'worldchain' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'across_v3_withdrawals',
    materialized = 'view',
    )
}}

{{across_v3_withdrawals(
    blockchain = blockchain
    , events = source('across_v3_worldchain', 'worldchain_spokepool_evt_filledv3relay')
    )}}