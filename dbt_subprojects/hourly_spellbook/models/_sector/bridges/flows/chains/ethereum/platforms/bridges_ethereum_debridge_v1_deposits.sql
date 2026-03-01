{% set blockchain = 'ethereum' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'debridge_v1_deposits',
    materialized = 'view',
    )
}}

{{debridge_v1_deposits(blockchain = blockchain)}}