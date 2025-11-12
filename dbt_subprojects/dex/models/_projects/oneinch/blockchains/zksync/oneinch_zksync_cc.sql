{%- set blockchain = oneinch_zksync_cfg_macro() -%}
{%- set stream = oneinch_cc_cfg_macro() -%}

{{-
    config(
        schema = 'oneinch_' + blockchain.name,
        alias = stream.name,
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['block_month', 'tx_hash', 'call_trace_address'],
    )
-}}

{{-
    oneinch_cc_macro(
        blockchain = blockchain,
        stream = stream,
        contracts = oneinch_zksync_cc_contracts_cfg_macro(),
        initial = oneinch_zksync_lo_contracts_cfg_macro(),
        type = "raw",
    )
-}}