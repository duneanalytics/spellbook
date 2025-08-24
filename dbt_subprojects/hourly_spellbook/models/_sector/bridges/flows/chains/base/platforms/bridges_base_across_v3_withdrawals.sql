{% set blockchain = 'base' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'across_v3_withdrawals',
    materialized = 'view',
    )
}}

{{across_v3_withdrawals(
    blockchain = blockchain
    , events = ref('across_v2_base.base_spokepool_evt_filledv3relay')
    )}}