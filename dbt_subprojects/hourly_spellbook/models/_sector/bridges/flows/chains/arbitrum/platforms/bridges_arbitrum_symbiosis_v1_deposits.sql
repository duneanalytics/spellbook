{% set blockchain = 'arbitrum' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'symbiosis_v1_deposits',
    materialized = 'view',
    )
}}

{{symbiosis_v1_deposits(
    blockchain = blockchain
    )}}