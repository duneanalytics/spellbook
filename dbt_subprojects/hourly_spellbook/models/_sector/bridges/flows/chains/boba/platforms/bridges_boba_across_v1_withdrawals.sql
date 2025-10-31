{% set blockchain = 'boba' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'across_v1_withdrawals',
    materialized = 'view',
    )
}}

{{across_v1_withdrawals(
    blockchain = blockchain
    , events = source('across_v2_boba', 'boba_spokepool_evt_filledrelay')
    )}}