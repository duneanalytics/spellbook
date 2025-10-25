{% set blockchain = 'unichain' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'across_v2_deposits',
    materialized = 'view',
    )
}}

{{across_v2_deposits(
    blockchain = blockchain
    , events = source('across_v3_unichain', 'unichain_spokepool_evt_fundsdeposited')
    )}}