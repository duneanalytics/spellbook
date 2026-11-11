{% set blockchain = 'fantom' %}

{{
    config(
        schema = 'tokens_' ~ blockchain,
        alias = 'transfers_from_traces_base_wrapper_deposits',
        partition_by = ['block_month'],
        materialized = 'table',
        tags = ['static'],
        file_format = 'delta',
    )
}}

{{ transfers_from_traces_base_wrapper_deposits_macro(blockchain=blockchain, transfers_from_traces_base_table=ref('tokens_' ~ blockchain ~ '_transfers_from_traces_base')) }}