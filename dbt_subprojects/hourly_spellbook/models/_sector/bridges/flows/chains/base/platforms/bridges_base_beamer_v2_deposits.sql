{% set blockchain = 'base' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'beamer_v2_deposits',
    materialized = 'view',
    )
}}

{{beamer_deposits(blockchain = blockchain
    , version = '2'
    )}}