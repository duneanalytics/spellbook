{%- set blockchain = 'avalanche_c' -%}

{{-
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'ar',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['block_month', 'tx_hash', 'call_trace_address'],
    )
-}}

{{- oneinch_ar_macro(
        blockchain = oneinch_avalanche_c_cfg_macro(),
        stream = oneinch_ar_cfg_macro(),
        contracts = oneinch_avalanche_c_ar_contracts_cfg_macro()
) -}}