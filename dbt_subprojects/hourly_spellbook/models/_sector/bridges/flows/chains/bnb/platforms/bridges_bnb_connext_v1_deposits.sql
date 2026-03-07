{% set blockchain = 'bnb' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'connext_v1_deposits',
    materialized = 'view',
    )
}}

{{connext_v1_deposits(blockchain = blockchain)}}