{% set blockchain = 'blast' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'across_v2_deposits',
    materialized = 'view',
    )
}}

{{across_v2_deposits(
    blockchain = blockchain
    , events = source('across_v3_blast', 'blast_spokepool_evt_fundsdeposited')
    )}}