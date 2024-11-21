{% set blockchain = 'bnb' %}



{{
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'call_transfers',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['tx_hash', 'call_trace_address', 'transfer_blockchain', 'transfer_tx_hash', 'transfer_trace_address', 'transfer_native'],
    )
}}



{{
    oneinch_call_transfers_macro(
        blockchain = blockchain
    )
}}
