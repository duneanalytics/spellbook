{%- set blockchain = oneinch_optimism_cfg_macro() -%}

{{-
    config(
        schema = 'oneinch_' + blockchain.name,
        alias = 'transfers',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['block_month', 'tx_hash', 'call_trace_address', 'transfer_trace_address', 'transfer_contract_address'],
    )
-}}

{{-
    oneinch_transfers_macro(
        blockchain = blockchain,
        streams = [
            oneinch_ar_transfers_cfg_macro(),
            oneinch_lo_transfers_cfg_macro(),
            oneinch_cc_transfers_cfg_macro(),
        ]
    )
-}}