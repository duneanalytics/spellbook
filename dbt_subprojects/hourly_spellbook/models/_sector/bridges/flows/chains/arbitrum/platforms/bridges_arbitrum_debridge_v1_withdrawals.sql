{% set blockchain = 'arbitrum' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'debridge_v1_withdrawals',
    materialized = 'view',
    )
}}

{{debridge_v1_withdrawals(blockchain = blockchain)}}