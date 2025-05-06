{{
    config(
        schema = 'oneinch_solana',
        alias = 'transfers',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['tx_id', 'order_hash', 'call_trace_address', 'transfer_trace_address']
    )
}}



select
    'solana' as blockchain
    , *
    , cast(date_trunc('month', block_time) as date) as block_month
from ({{ oneinch_fusion_transfers_macro() }})
