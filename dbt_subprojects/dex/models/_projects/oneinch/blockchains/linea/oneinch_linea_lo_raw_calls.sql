{%- set blockchain = 'linea' -%}

{{-
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'lo_raw_calls',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['block_month', 'tx_hash', 'call_trace_address'],
    )
-}}

{{- oneinch_raw_calls_macro(
        blockchain = oneinch_linea_cfg_macro(),
        stream = oneinch_lo_raw_calls_cfg_macro(),
        contracts = oneinch_linea_lo_contracts_cfg_macro()
) -}}