{% set blockchain = 'hyperevm' %}

{{
    config(
        schema = 'addresses_' + blockchain,
        alias = 'stats',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        merge_skip_unchanged = true,
        unique_key = ['blockchain', 'address']
    )
}}

{{ addresses_stats(blockchain) }}
