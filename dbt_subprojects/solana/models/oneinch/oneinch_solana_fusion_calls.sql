{{
    config(
        schema = 'oneinch_solana',
        alias = 'fusion_calls',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['tx_id', 'order_hash', 'call_trace_address']
    )
}}



{% set discriminator = 'substr(data, 1, 8)' %}


select 
    'solana' as blockchain
    , block_slot
    , block_date
    , block_time
    , block_hash
    , tx_index
    , inner_instruction_index
    , outer_instruction_index
    , inner_executing_account
    , outer_executing_account
    , executing_account
    , tx_id
    , tx_signer
    , tx_success
    , data
    -- This info extracted from code
    , case {{ discriminator }}
        when {{ oneinch_cfg_macro("create_discriminator") }} then 'create'
        when {{ oneinch_cfg_macro("fill_discriminator") }} then 'fill'
        when {{ oneinch_cfg_macro("cancel_discriminator") }} then 'cancel'
    end as instruction_type -- ?? name
    , {{ discriminator }} as discriminator -- 8 bytes
    , bytearray_to_bigint(bytearray_reverse(substr(data, 9, 4))) as order_id -- 4 bytes (u32)
    , bytearray_to_bigint(bytearray_reverse(substr(data, 9 + 4, 8))) as src_amount -- 8 bytes (u64)
    , bytearray_to_bigint(bytearray_reverse(substr(data, 13 + 8, 8))) as min_dst_amount -- 8 bytes (u64)
    , bytearray_to_bigint(bytearray_reverse(substr(data, 21 + 8, 8))) as estimated_dst_amount -- 8 bytes (u64)
    , bytearray_to_bigint(bytearray_reverse(substr(data, 29 + 8, 4))) as expiration_time -- 4 bytes (u32)
    , bytearray_to_bigint(bytearray_reverse(substr(data, 37 + 4, 1))) as native_dst_asset -- 1 byte (bool)
    , bytearray_to_bigint(bytearray_reverse(substr(data, 41+1, 2))) as protocol_fee -- u16
    , bytearray_to_bigint(bytearray_reverse(substr(data, 42+2, 2))) as integrator_fee -- u16
    , bytearray_to_bigint(bytearray_reverse(substr(data, 44+2, 1))) as surplus_percentage -- u8
    , bytearray_to_bigint(bytearray_reverse(substr(data, 46+1, 4))) as start_time --u32
    , bytearray_to_bigint(bytearray_reverse(substr(data, 47+4, 4))) as duration --u32
    , bytearray_to_bigint(bytearray_reverse(substr(data, 51+4, 2))) as initial_rate_bump --u16
    -- In between here is PointAndDelta structure
    , bytearray_to_bigint(bytearray_reverse(substr(data, -8))) as amount

    -- ED: this needs to be simplified SOMEHOW as it's not sustainable
    , case {{ discriminator }}
        when {{ oneinch_cfg_macro("create_discriminator") }} -- create
            then case 
                    when account_arguments[8]  = '{{ oneinch_cfg_macro("fusion_program_id") }}' and  account_arguments[9] = '{{ oneinch_cfg_macro("fusion_program_id") }}'
                        then sha256(substr(data, 9, 33)  || coalesce(try(from_base58(account_arguments[5])), 0x00) || 0x00 || 0x00 || substr(data, 42) || coalesce(try(from_base58(account_arguments[2])), 0x00) || coalesce(try(from_base58(account_arguments[3])), 0x00))
                    when  account_arguments[8]  = '{{ oneinch_cfg_macro("fusion_program_id") }}' 
                        then sha256(substr(data, 9, 33)  || coalesce(try(from_base58(account_arguments[5])), 0x00) || 0x00 || coalesce(try(from_base58(account_arguments[9])), 0x00) || substr(data, 42) || coalesce(try(from_base58(account_arguments[2])), 0x00) || coalesce(try(from_base58(account_arguments[3])), 0x00))
                    when account_arguments[9] = '{{ oneinch_cfg_macro("fusion_program_id") }}'
                        then sha256(substr(data, 9, 33)  || coalesce(try(from_base58(account_arguments[5])), 0x00) || coalesce(try(from_base58(account_arguments[8])), 0x00) || 0x00  || substr(data, 42) || coalesce(try(from_base58(account_arguments[2])), 0x00) || coalesce(try(from_base58(account_arguments[3])), 0x00))
                    else
                        sha256(substr(data, 9, 33)  || coalesce(try(from_base58(account_arguments[5])), 0x00) || coalesce(try(from_base58(account_arguments[8])), 0x00)  || coalesce(try(from_base58(account_arguments[9])), 0x00) || substr(data, 42) || coalesce(try(from_base58(account_arguments[2])), 0x00) || coalesce(try(from_base58(account_arguments[3])), 0x00))
                end
        when {{ oneinch_cfg_macro("fill_discriminator") }} -- fill
            then case 
                    when account_arguments[10]  = '{{ oneinch_cfg_macro("fusion_program_id") }}' and  account_arguments[11] = '{{ oneinch_cfg_macro("fusion_program_id") }}'
                        then sha256(substr(data, 9, 33)  || coalesce(try(from_base58(account_arguments[4])), 0x00) || 0x00 || 0x00 || substr(data, 42, length(data)-41-8)  || coalesce(try(from_base58(account_arguments[5])), 0x00) || coalesce(try(from_base58(account_arguments[6])), 0x00))
                    when  account_arguments[10]  = '{{ oneinch_cfg_macro("fusion_program_id") }}' 
                        then sha256(substr(data, 9, 33)  || coalesce(try(from_base58(account_arguments[4])), 0x00) || 0x00 || coalesce(try(from_base58(account_arguments[11])), 0x00)  || substr(data, 42, length(data)-41-8)  || coalesce(try(from_base58(account_arguments[5])), 0x00) || coalesce(try(from_base58(account_arguments[6])), 0x00))
                    when account_arguments[11] = '{{ oneinch_cfg_macro("fusion_program_id") }}'
                        then sha256(substr(data, 9, 33)  || coalesce(try(from_base58(account_arguments[4])), 0x00) || coalesce(try(from_base58(account_arguments[10])), 0x00) || 0x00 || substr(data, 42, length(data)-41-8)  || coalesce(try(from_base58(account_arguments[5])), 0x00) || coalesce(try(from_base58(account_arguments[6])), 0x00))
                    else   
                        sha256(substr(data, 9, 33)  || coalesce(try(from_base58(account_arguments[4])), 0x00) || coalesce(try(from_base58(account_arguments[10])), 0x00)  || coalesce(try(from_base58(account_arguments[11])), 0x00) || substr(data, 42, length(data)-41-8)  || coalesce(try(from_base58(account_arguments[5])), 0x00) || coalesce(try(from_base58(account_arguments[6])), 0x00))
                end
        when {{ oneinch_cfg_macro("cancel_discriminator") }} -- cancel
            then substr(data, 9)
    end as order_hash
    --, substr(data, 9, 33)  || coalesce(try(from_base58(account_arguments[4])), 0x00) || 0x00 || 0x00 || substr(data, 42, length(data)-41-8) || coalesce(try(from_base58(account_arguments[5])), 0x00) || coalesce(try(from_base58(account_arguments[6])), 0x00) as order_hash_str
    --, length(data) as len_data
    -- This is extracted from IDL
    , account_arguments[1] as taker
    , account_arguments[2] as resolver_access
    , case {{ discriminator }}
        when {{ oneinch_cfg_macro("create_discriminator") }} then account_arguments[1] 
        when {{ oneinch_cfg_macro("cancel_discriminator") }} then account_arguments[1]
        when {{ oneinch_cfg_macro("fill_discriminator") }} then account_arguments[3] 
    end as maker
    , case {{ discriminator }}
        when {{ oneinch_cfg_macro("create_discriminator") }} then account_arguments[5] 
        when {{ oneinch_cfg_macro("cancel_discriminator") }} then null
        when {{ oneinch_cfg_macro("fill_discriminator") }} then account_arguments[4] 
    end as maker_receiver
    , case {{ discriminator }}
        when {{ oneinch_cfg_macro("create_discriminator") }} then account_arguments[2] 
        when {{ oneinch_cfg_macro("cancel_discriminator") }} then account_arguments[2]
        when {{ oneinch_cfg_macro("fill_discriminator") }} then account_arguments[5] 
    end as src_mint
    , case {{ discriminator }}
        when {{ oneinch_cfg_macro("create_discriminator") }} then account_arguments[3] 
        when {{ oneinch_cfg_macro("cancel_discriminator") }} then null
        when {{ oneinch_cfg_macro("fill_discriminator") }} then account_arguments[6] 
    end as dst_mint
    , case {{ discriminator }}
        when {{ oneinch_cfg_macro("create_discriminator") }} then account_arguments[6] 
        when {{ oneinch_cfg_macro("cancel_discriminator") }} then account_arguments[3]
        when {{ oneinch_cfg_macro("fill_discriminator") }} then account_arguments[7] 
    end as escrow
    -- , try(account_arguments[8]) as escrow_src_ata -- TODO: LATER
    -- , try(account_arguments[9]) as make_dst_ata
    -- , try(account_arguments[10]) as protocol_dst_ata
    -- , try(account_arguments[11]) as integrator_dst_ata
    -- , try(account_arguments[12]) as taker_src_ata
    -- , try(account_arguments[13]) as taker_dst_ata
    -- , try(account_arguments[14]) as src_token_program
    -- , try(account_arguments[15]) as dst_token_program
    -- , try(account_arguments[16]) as system_program
    -- , try(account_arguments[17]) as associated_token_program
    , array[coalesce(outer_instruction_index, -1), coalesce(inner_instruction_index, -1)] as call_trace_address
    , cast(date_trunc('month', block_time) as date) as block_month
from {{ source('solana', 'instruction_calls') }} 
where 
    {% if is_incremental() %}
        {{ incremental_predicate('block_time') }}
    {% else %}
        block_time >= date('{{ oneinch_cfg_macro("project_start_date") }}')
    {% endif %}
    and executing_account = '{{ oneinch_cfg_macro("fusion_program_id") }}' -- ED: changed as this columns is partitioned TODO: CHECK LATER
    and not is_inner
    and {{ discriminator }} in (
        {{ oneinch_cfg_macro("create_discriminator") }} -- create
        , {{ oneinch_cfg_macro("fill_discriminator") }} -- fill
        , {{ oneinch_cfg_macro("cancel_discriminator") }} -- cancel
    ) 


