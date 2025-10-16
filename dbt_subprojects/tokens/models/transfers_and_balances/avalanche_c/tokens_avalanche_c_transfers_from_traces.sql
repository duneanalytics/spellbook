{% set blockchain = 'avalanche_c' %}

{{
    config(
        schema = 'tokens_' ~ blockchain,
        alias = 'transfers_from_traces',
        materialized = 'view',
    )
}}

{{ transfers_from_traces_macro(blockchain=blockchain) }}