{% set blockchain = 'base' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'butter_v2_deposits',
    materialized = 'view',
    )
}}

{{butter_v2_deposits(blockchain = blockchain)}}