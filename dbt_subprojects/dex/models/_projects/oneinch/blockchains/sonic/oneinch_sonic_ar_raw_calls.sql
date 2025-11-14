{%- set blockchain = oneinch_sonic_cfg_macro() -%}
{%- set stream = oneinch_ar_raw_calls_cfg_macro() -%}

{{-
    config(
        schema = 'oneinch_' + blockchain.name,
        alias = stream.name + '_raw_calls',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['block_month', 'tx_hash', 'call_trace_address'],
    )
-}}

{{-
    oneinch_raw_calls_macro(
        blockchain = blockchain,
        stream = stream,
        contracts = oneinch_sonic_ar_contracts_cfg_macro()
    )
-}}