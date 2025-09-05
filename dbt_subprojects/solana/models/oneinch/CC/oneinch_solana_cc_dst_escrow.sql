{{
    config(
        schema = 'oneinch_solana',
        alias = 'cc_dst_escrow',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['block_month', 'account_escrow', 'hashlock']
    )
}}



with 


dst_escrow_create as (
    select
        account_escrow
        , account_mint dst_token_address
        , amount dst_amount
        , assetIsNative dst_is_native
        , recipient as dst_user
        , {{ oneinch_solana_array_to_hex_macro('hashlock') }} hashlock
        , {{ oneinch_solana_array_to_hex_macro('orderHash') }} order_hash
        , call_tx_id as create_escrow_tx_id
        , call_block_slot as create_escrow_block_slot
        , call_block_time as create_escrow_block_time
        , call_outer_instruction_index as create_escrow_outer_instruction_index
        , array[coalesce(call_outer_instruction_index, -1), coalesce(call_inner_instruction_index, -1)] create_escrow_call_trace_address
    from {{ source('oneinch_solana', 'crossChainEscrowDst_call_create') }}
    where 
    {% if is_incremental() %}
        {{ incremental_predicate('call_block_time') }}
    {% else %}
        call_block_time >= date('{{ oneinch_solana_cfg_macro("cc_start_date") }}')
    {% endif %}
)

, dst_escrow_withdraw as (
    select 
        account_escrow
        , call_tx_id as withdraw_tx_id
        , call_block_slot as withdraw_block_slot
        , call_block_time as withdraw_block_time
        , call_outer_instruction_index as withdraw_outer_instruction_index
        , array[coalesce(call_outer_instruction_index, -1), coalesce(call_inner_instruction_index, -1)] withdraw_call_trace_address
    from {{ source('oneinch_solana', 'crossChainEscrowDst_call_withdraw') }}
    where 
    {% if is_incremental() %}
        {{ incremental_predicate('call_block_time') }}
    {% else %}
        call_block_time >= date('{{ oneinch_solana_cfg_macro("cc_start_date") }}')
    {% endif %}
)


select 
    account_escrow
    , create_escrow_tx_id
    , create_escrow_block_slot
    , create_escrow_block_time
    , create_escrow_outer_instruction_index
    , create_escrow_call_trace_address
    , dst_user
    , dst_token_address
    , dst_is_native
    , dst_amount
    , hashlock
    , order_hash
    , withdraw_tx_id
    , withdraw_block_slot
    , withdraw_block_time
    , withdraw_outer_instruction_index
    , withdraw_call_trace_address
    , case
        when withdraw_tx_id is null then 'created_escrow'
        else 'withdrawn'
    end as status
    , cast(date_trunc('month', create_escrow_block_time) as date) as block_month
from dst_escrow_create
left join dst_escrow_withdraw using(account_escrow)