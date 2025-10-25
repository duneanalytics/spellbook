{% set blockchain = 'plasma' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'across_v3_withdrawals',
    materialized = 'view',
    )
}}

{{across_v3_withdrawals(
    blockchain = blockchain
    , events = source('across_plasma', 'universal_spokepool_evt_filledrelay')
    )}}