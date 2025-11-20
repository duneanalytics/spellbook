{% set blockchain = 'blast' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'across_v2_withdrawals',
    materialized = 'view',
    )
}}

{{across_v2_withdrawals(
    blockchain = blockchain
    , events = source('across_v3_blast', 'blast_spokepool_evt_filledrelay')
    )}}