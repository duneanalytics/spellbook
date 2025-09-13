{% set blockchain = 'bnb' %}

{{
    config(
        schema = 'tokens_' ~ blockchain,
        alias = 'transfers_traces',
        materialized = 'view',
    )
}}



{{ transfers_traces(blockchain=blockchain) }}
