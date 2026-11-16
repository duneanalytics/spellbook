{% set blockchain = 'shape' %}

{{
    config(
        schema = 'tokens_' ~ blockchain,
        alias = 'transfers_from_traces',
        partition_by = ['block_month'],
        materialized = 'table',
        tags = ['static'],
        file_format = 'delta',
    )
}}

{{ transfers_from_traces_macro(blockchain=blockchain, transfers_start_date='2024-07-23') }}