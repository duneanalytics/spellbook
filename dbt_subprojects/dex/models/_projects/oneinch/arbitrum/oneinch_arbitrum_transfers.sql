{% set blockchain = 'arbitrum' %}



{{
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'transfers',
        partition_by = ['block_month', 'block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['blockchain', 'tx_hash', 'transfer_trace_address', 'contract_address']
    )
}}



{{
    oneinch_transfers_macro(
        blockchain = blockchain
    )
}}