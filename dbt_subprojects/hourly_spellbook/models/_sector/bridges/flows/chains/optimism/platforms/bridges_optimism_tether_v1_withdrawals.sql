{% set blockchain = 'optimism' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'tether_v1_withdrawals',
    materialized = 'view',
    )
}}

{{tether_v1_withdrawals(blockchain = blockchain)}}