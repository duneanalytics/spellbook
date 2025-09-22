{% set blockchain = 'bnb' %}
{% set stream = 'lo' %}

{{
    config(
        schema = 'oneinch_' + blockchain,
        alias = stream,
        partition_by = ['block_month', 'block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['tx_hash', 'call_trace_address'],
    )
}}

{{ oneinch_lo_macro(blockchain = blockchain, for_stream = stream) }}