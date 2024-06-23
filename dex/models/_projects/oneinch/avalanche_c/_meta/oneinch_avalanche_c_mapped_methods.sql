{% set blockchain = 'avalanche_c' %}



{{
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'mapped_methods',
        materialized = 'table',
        unique_key = ['blockchain', 'address'],
    )
}}



{{ oneinch_mapped_methods_macro(blockchain) }}