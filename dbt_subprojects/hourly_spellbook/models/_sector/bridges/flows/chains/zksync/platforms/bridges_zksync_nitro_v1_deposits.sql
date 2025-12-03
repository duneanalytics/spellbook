{% set blockchain = 'zksync' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'nitro_v1_deposits',
    materialized = 'view',
    )
}}

{{nitro_v1_deposits(blockchain = blockchain)}}