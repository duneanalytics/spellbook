{%- set blockchain = oneinch_unichain_cfg_macro() -%}
{%- set stream = oneinch_lo_cfg_macro() -%}

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
    oneinch_lo_macro(
        blockchain = blockchain,
        stream = stream,
        contracts = oneinch_unichain_lo_contracts_cfg_macro()
    )
-}}