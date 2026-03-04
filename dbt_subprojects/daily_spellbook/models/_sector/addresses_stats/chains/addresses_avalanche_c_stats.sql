{% set blockchain = 'avalanche_c' %}

{{
    config(
        schema = 'addresses_' + blockchain,
        alias = 'stats',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'address']
    )
}}

{{ addresses_stats(blockchain) }}
