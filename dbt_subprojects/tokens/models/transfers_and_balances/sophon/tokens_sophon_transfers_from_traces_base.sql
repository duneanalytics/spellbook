{% set blockchain = 'sophon' %}

{{
    config(
        schema = 'tokens_' ~ blockchain,
        alias = 'transfers_from_traces_base',
        partition_by = ['block_month'],
        materialized = 'table',
        tags = ['static'],
        file_format = 'delta',
    )
}}

{{ transfers_from_traces_base_macro(blockchain=blockchain) }}