{%- set blockchain = 'sonic' -%}

{{-
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'cc',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['block_month', 'tx_hash', 'call_trace_address'],
    )
-}}

{{- oneinch_cc_macro(
        blockchain = oneinch_sonic_cfg_macro(),
        stream = oneinch_cc_cfg_macro(),
        contracts = oneinch_sonic_cc_contracts_cfg_macro(),
        initial = oneinch_sonic_lo_contracts_cfg_macro()
) -}}