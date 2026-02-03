{% set blockchain = 'ethereum' %}

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
