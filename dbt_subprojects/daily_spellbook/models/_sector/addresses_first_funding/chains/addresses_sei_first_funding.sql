{% set blockchain = 'sei' %}

{{
    config(
        schema = 'addresses_' + blockchain,
        alias = 'first_funding',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['address']
    )
}}

{{
    addresses_first_funding(blockchain)
}}
