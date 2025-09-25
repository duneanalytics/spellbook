{% set blockchain = 'avalanche_c' %}

{{
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'lo_executions',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['tx_hash', 'call_trace_address'],
    )
}}

{{ oneinch_lo_executions_macro(blockchain = blockchain) }}