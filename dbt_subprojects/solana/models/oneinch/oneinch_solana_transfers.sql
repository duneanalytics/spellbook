{{
    config(
        schema = 'oneinch_solana',
        alias = 'transfers',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['tx_id', 'order_hash', 'call_trace_address']
    )
}}



select
    'solana' as blockchain
    , true as fusion -- TODO: make flags
    , *
    , coalesce(src_token_amount_usd, dst_token_amount_usd) as amount_usd
    , cast(date_trunc('month', block_time) as date) as block_month
from ({{ oneinch_fusion_transfers_macro('src') }})
inner join ({{ oneinch_fusion_transfers_macro('dst') }}) using(tx_id, block_time, order_hash, user, resolver, block_slot, tx_success, call_trace_address)

