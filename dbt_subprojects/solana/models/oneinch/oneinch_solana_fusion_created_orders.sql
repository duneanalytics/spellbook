{{
    config(
        schema = 'oneinch_solana',
        alias = 'fusion_created_orders',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['block_month', 'tx_id', 'order_hash']
    )
}}


select 
    'solana' as blockchain
    , call_block_slot as block_slot
    , call_block_date as block_date
    , call_block_time as block_time
    , call_block_hash as block_hash
    , call_tx_index as tx_index
    , call_inner_instruction_index as inner_instruction_index
    , call_outer_instruction_index as outer_instruction_index
    , call_inner_executing_account as inner_executing_account
    , call_outer_executing_account as outer_executing_account
    , call_executing_account as executing_account
    , call_is_inner as is_inner
    , call_program_name as program_name
    , call_instruction_name as instruction_name
    , call_version as version
    , call_data as data
    , call_tx_id as tx_id
    , call_tx_signer as tx_signer
    , {{ oneinch_order_hash_macro() }} as order_hash
    , to_base58({{ oneinch_order_hash_macro() }}) as order_hash_base58
    , cast(json_value("order", 'lax $.OrderConfig.id') as uint256) as order_id
    , cast(json_value("order", 'lax $.OrderConfig.src_amount') as uint256) as order_src_amount
    , cast(json_value("order", 'lax $.OrderConfig.min_dst_amount') as uint256) as order_min_dst_amount
    , cast(json_value("order", 'lax $.OrderConfig.estimated_dst_amount') as uint256) as order_estimated_dst_amount
    , from_unixtime(cast(json_value("order", 'lax $.OrderConfig.expiration_time') as bigint)) as order_expiration_time
    , cast(json_value("order", 'lax $.OrderConfig.src_asset_is_native') as boolean) as order_src_asset_is_native
    , cast(json_value("order", 'lax $.OrderConfig.dst_asset_is_native') as boolean) as order_dst_asset_is_native
    , account_system_program as system_program
    , account_escrow as escrow
    , account_src_mint as src_mint
    , account_src_token_program as src_token_program
    , account_escrow_src_ata as escrow_src_ata
    , account_maker as maker
    , account_maker_src_ata as maker_src_ata
    , account_dst_mint as dst_mint
    , account_maker_receiver as maker_receiver
    , account_associated_token_program as associated_token_program
    , account_protocol_dst_acc as protocol_dst_acc
    , account_integrator_dst_acc as integrator_dst_acc
    , "order"
    , cast(date_trunc('month', call_block_time) as date) as block_month
from {{ source('oneinch_solana', 'fusion_swap_call_create') }}
where 
    {% if is_incremental() %}
        {{ incremental_predicate('call_block_time') }}
    {% else %}
        call_block_time >= date('{{ oneinch_cfg_macro("project_start_date") }}')
    {% endif %}
