{% set blockchain = 'bnb' %}

{{
    config(
        tags=['prod_exclude'],
        schema = 'tokens_' + blockchain,
        alias = 'info',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['token_address']
    )
}}

{{
    addresses_info(
        blockchain = blockchain
    )
}}
