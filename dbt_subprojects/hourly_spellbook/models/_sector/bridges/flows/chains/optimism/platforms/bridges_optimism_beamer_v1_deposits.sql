{% set blockchain = 'optimism' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'beamer_v1_deposits',
    materialized = 'view',
    )
}}

{{beamer_deposits(blockchain = blockchain
    , version = '1'
    )}}