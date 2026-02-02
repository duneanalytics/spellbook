{% set blockchain = 'zksync' %}

{{
    config(
        schema = 'addresses_' + blockchain,
        alias = 'stats',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['address']
    )
}}

{{
    addresses_stats(blockchain)
}}
