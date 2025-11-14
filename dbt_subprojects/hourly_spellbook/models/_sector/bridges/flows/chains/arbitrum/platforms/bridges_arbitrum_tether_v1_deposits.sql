{% set blockchain = 'arbitrum' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'tether_v1_deposits',
    materialized = 'view',
    )
}}

{{tether_v1_deposits(blockchain = blockchain)}}