{% set blockchain = 'bnb' %}
{% set stream = 'ar' %}

{{
    config(
        schema = 'oneinch_' + blockchain,
        alias = stream + '_raw_calls',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['tx_hash', 'call_trace_address'],
    )
}}

{{ oneinch_raw_calls_macro(blockchain = blockchain, stream = stream) }}