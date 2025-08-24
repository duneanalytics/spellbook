{% set blockchain = 'ethereum' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'across_v3_withdrawals',
    materialized = 'view',
    )
}}

{{across_v3_withdrawals(
    blockchain = blockchain
    , events = ref('across_v2_ethereum.uba_ethereum_spokepool_evt_filledv3relay')
    )}}