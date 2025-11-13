{% set blockchain = 'corn' %}

{{
    config(
        schema = 'tokens_' ~ blockchain,
        alias = 'transfers_from_traces',
        materialized = 'view',
    )
}}

{{ transfers_from_traces_macro(blockchain=blockchain, transfers_start_date='2024-11-19') }}