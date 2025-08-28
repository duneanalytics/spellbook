{{
    config(
        schema = 'oneinch_solana',
        alias = 'cc_src_escrow',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['block_month', 'account_order', 'hashlock']
    )
}}



with 

src_escrow_create as (
    select
        account_order
        , amount order_src_amount
        , assetIsNative src_is_native
        , {{ oneinch_solana_array_to_hex_macro('hashlock') }} initial_hashlock
        , cast(json_value(dstChainParams, 'lax $.dstChainParams.chainId') as bigint) dst_chain_id
        , substr({{ oneinch_solana_array_to_hex_macro("cast(json_parse(json_query(dstChainParams, 'lax $.dstChainParams.makerAddress')) as array(bigint))") }}, 13, 20) dst_user
        , call_tx_id as create_tx_id
        , call_block_slot as create_block_slot
        , call_block_time as create_block_time
        , call_outer_instruction_index as create_outer_instruction_index
        , array[coalesce(call_outer_instruction_index, -1), coalesce(call_inner_instruction_index, -1)] create_call_trace_address
    from {{ source('oneinch_solana', 'crossChainEscrowSrc_call_create') }}
    where 
    {% if is_incremental() %}
        {{ incremental_predicate('call_block_time') }}
    {% else %}
        call_block_time >= date('{{ oneinch_solana_cfg_macro("cc_start_date") }}')
    {% endif %}
)

, src_escrow_createEscrow as (
    select
        account_escrow
        , account_maker as src_user
        , account_mint as src_token_mint
        , account_order
        , amount as src_amount
        , {{ oneinch_solana_array_to_hex_macro("cast(json_parse(json_query(merkleProof, 'lax $.merkleProof.hashedSecret')) as array(bigint))") }} hashed_secret
        , call_tx_id as create_escrow_tx_id
        , call_block_slot as create_escrow_block_slot
        , call_block_time as create_escrow_block_time
        , call_outer_instruction_index as create_escrow_outer_instruction_index
        , array[coalesce(call_outer_instruction_index, -1), coalesce(call_inner_instruction_index, -1)] create_escrow_call_trace_address
    from {{ source('oneinch_solana', 'crossChainEscrowSrc_call_createEscrow') }}
    where 
    {% if is_incremental() %}
        {{ incremental_predicate('call_block_time') }}
    {% else %}
        call_block_time >= date('{{ oneinch_solana_cfg_macro("cc_start_date") }}')
    {% endif %}
)

, src_escrow_withdraw as (
    select 
        call_block_time src_block_time
        , account_escrow
        , call_tx_id as withdraw_tx_id
        , call_block_slot as withdraw_block_slot
        , call_block_time as withdraw_block_time
        , call_outer_instruction_index as withdraw_outer_instruction_index
        , array[coalesce(call_outer_instruction_index, -1), coalesce(call_inner_instruction_index, -1)] withdraw_call_trace_address
    from {{ source('oneinch_solana', 'crossChainEscrowSrc_call_withdraw') }}
    where 
    {% if is_incremental() %}
        {{ incremental_predicate('call_block_time') }}
    {% else %}
        call_block_time >= date('{{ oneinch_solana_cfg_macro("cc_start_date") }}')
    {% endif %}
)

, blockchains as (
    select 
        chain_id as dst_chain_id
        , blockchain as dst_blockchain 
    from {{ source('oneinch', 'blockchains') }}
)


select 
    account_order
    , create_tx_id
    , create_block_slot
    , create_block_time
    , create_outer_instruction_index
    , create_call_trace_address
    , dst_chain_id
    , dst_blockchain
    , src_user
    , dst_user
    , account_escrow
    , create_escrow_tx_id
    , create_escrow_block_slot
    , create_escrow_block_time
    , create_escrow_outer_instruction_index
    , create_escrow_call_trace_address
    , src_token_mint
    , src_is_native
    , order_src_amount
    , src_amount
    , withdraw_tx_id
    , withdraw_block_slot
    , withdraw_block_time
    , withdraw_outer_instruction_index
    , withdraw_call_trace_address
    , case
        when create_escrow_tx_id is null then 'created'
        when withdraw_tx_id is null then 'created_escrow'
        else 'withdrawn'
    end as status
    , coalesce(hashed_secret, initial_hashlock) hashlock -- in cases of partial fills hashlock is changing, so we take hashed secret if presented
    , cast(date_trunc('month', create_block_time) as date) as block_month
from src_escrow_create
join blockchains using(dst_chain_id)
left join src_escrow_createEscrow using(account_order)
left join src_escrow_withdraw using(account_escrow)

