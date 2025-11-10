{% set blockchain = 'ethereum' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'beamer_v2_deposits',
    materialized = 'view',
    )
}}

{{beamer_v2_deposits(blockchain = blockchain)}}