{% set blockchain = 'lens' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'across_v2_deposits',
    materialized = 'view',
    )
}}

{{across_v2_deposits(
    blockchain = blockchain
    , events = source('across_v3_lens', 'lens_spokepool_evt_fundsdeposited')
    )}}