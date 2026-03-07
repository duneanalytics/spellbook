{%- set blockchain = oneinch_fantom_cfg_macro() -%}
{%- set stream = oneinch_ar_executions_cfg_macro() -%}

{{-
    config(
        schema = 'oneinch_' + blockchain.name,
        alias = stream.name + '_executions',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['block_month', 'tx_hash', 'call_trace_address'],
    )
-}}

{{-
    oneinch_ar_executions_macro(
        blockchain = blockchain,
        stream = stream
    )
-}}