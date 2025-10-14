{% set blockchain = 'optimism' %}

{{
    config(
        schema = 'tokens_' ~ blockchain,
        alias = 'transfers_from_traces_base',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['block_date', 'unique_key'],
    )
}}

{{ transfers_from_traces_base_macro(blockchain=blockchain, easy_dates=true) }}