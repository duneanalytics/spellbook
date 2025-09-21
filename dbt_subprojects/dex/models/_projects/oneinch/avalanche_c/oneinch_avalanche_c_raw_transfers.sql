{% set blockchain = 'avalanche_c' %}

{{
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'raw_transfers',
        partition_by = ['block_month', 'block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['tx_hash', 'call_trace_address', 'transfer_trace_address', 'transfer_contract_address'],
    )
}}

{{ oneinch_raw_transfers_macro(blockchain) }}