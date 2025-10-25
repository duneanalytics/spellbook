{% set blockchain = 'scroll' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'across_v2_withdrawals',
    materialized = 'view',
    )
}}

{{across_v2_withdrawals(
    blockchain = blockchain
    , events = source('across_v3_scroll', 'scroll_spokepool_evt_filledrelay')
    )}}