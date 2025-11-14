{% set blockchain = 'bnb' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'zkbridge_v1_deposits',
    materialized = 'view',
    )
}}

{{zkbridge_v1_deposits(blockchain = blockchain)}}