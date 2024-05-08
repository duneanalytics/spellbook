{% set blockchain = 'avalanche_c' %}



{{
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'mapped_methods',
        materialized = 'table',
        on_table_exists = 'drop',
        unique_key = ['blockchain', 'address'],
    )
}}



{{ oneinch_mapped_methods_macro(blockchain) }}