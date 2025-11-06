{%- set blockchain = 'zksync' -%}

{{-
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'lo',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['block_month', 'tx_hash', 'call_trace_address'],
    )
-}}

{{- oneinch_lo_macro(
        blockchain = oneinch_zksync_cfg_macro(),
        stream = oneinch_lo_cfg_macro(),
        contracts = oneinch_zksync_lo_contracts_cfg_macro()
) -}}