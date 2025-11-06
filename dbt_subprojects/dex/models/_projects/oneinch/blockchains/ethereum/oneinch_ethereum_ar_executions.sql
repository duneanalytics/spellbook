{%- set blockchain = 'ethereum' -%}

{{-
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'ar_executions',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['block_month', 'tx_hash', 'call_trace_address'],
    )
-}}

{{- oneinch_ar_executions_macro(
        blockchain = oneinch_ethereum_cfg_macro(),
        stream = oneinch_ar_executions_cfg_macro()
) -}}