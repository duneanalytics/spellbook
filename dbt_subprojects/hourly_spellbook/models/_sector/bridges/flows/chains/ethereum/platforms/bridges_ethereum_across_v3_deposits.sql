{% set blockchain = 'ethereum' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'across_v3_deposits',
    materialized = 'view',
    )
}}

{{across_v3_deposits(
    blockchain = blockchain
    , events = ref('across_v2_ethereum.uba_ethereum_spokepool_evt_v3fundsdeposited')
    )}}