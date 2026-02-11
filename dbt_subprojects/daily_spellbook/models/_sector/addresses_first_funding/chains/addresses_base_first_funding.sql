{% set blockchain = 'base' %}

{{
    config(
        schema = 'addresses_' + blockchain,
        alias = 'first_funding',
        materialized = 'view'
    )
}}

{{
    addresses_first_funding(blockchain)
}}
