{% macro oneinch_fusion_transfers_macro() %} -- src/dst


select 
        tx_id
        , block_time
        , block_slot
        , order_hash
        , order_hash_base58
        , order_id
        , token_mint_address
        , taker
        , symbol 
        , amount 
        , amount_usd 
        , from_owner
        , to_owner
        , call_trace_address
        , array[coalesce(outer_instruction_index, -1), coalesce(inner_instruction_index, -1)] as transfer_trace_address
from (
    select 
        call_tx_id as tx_id
        , call_block_time as block_time
        , call_block_slot as block_slot
        , account_taker as taker
        , {{ oneinch_order_hash_macro() }} as order_hash
        , to_base58({{ oneinch_order_hash_macro() }}) as order_hash_base58
        , cast(json_value("order", 'lax $.OrderConfig.id') as uint256) as order_id
        , call_outer_instruction_index as outer_instruction_index
        , array[coalesce(call_outer_instruction_index, -1), coalesce(call_inner_instruction_index, -1)] call_trace_address
    from {{ source('oneinch_solana', 'fusion_swap_call_fill') }}
    where 
        {% if is_incremental() %}
            {{ incremental_predicate('call_block_time') }}
        {% else %}
            call_block_time >= date('{{ oneinch_cfg_macro("project_start_date") }}')
        {% endif %}
) qf
left join (
        select * from {{ source('tokens_solana','transfers') }}
        {% if is_incremental() %}
            where {{ incremental_predicate('block_time') }}
        {% else %}
            where block_time >= date('{{ oneinch_cfg_macro("project_start_date") }}')
        {% endif %}
) using(tx_id, block_time, block_slot, outer_instruction_index)
where 
    action = 'transfer' -- There are also mint/burn, maybe we need it? So guess no.
    -- TODO: evaluate if we need this condition or not? 
    --and tst.outer_executing_account = (select fusion_program_id from static)
    {% if is_incremental() %}
        and {{ incremental_predicate('block_time') }}
    {% else %}
        and block_time >= date('{{ oneinch_cfg_macro("project_start_date") }}')
    {% endif %}


{% endmacro %}