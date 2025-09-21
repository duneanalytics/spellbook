{% set blockchain = 'fantom' %}
{% set stream = 'lo_raw_calls' %}

{{
    config(
        schema = 'oneinch_' + blockchain,
        alias = stream,
        partition_by = ['block_month', 'block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['tx_hash', 'trace_address'],
    )
}}

{{
    oneinch_raw_calls_macro(
        blockchain = blockchain,
        stream = stream,
    )
}}