{% set blockchain = 'optimism' %}



{{
    config(
        tags = ['prod_exclude'],
        schema = 'oneinch_' + blockchain,
        alias = 'mapped_contracts',
        materialized = 'table',
        unique_key = ['blockchain', 'address'],
    )
}}



{{ oneinch_mapped_contracts_macro(blockchain) }}