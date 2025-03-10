{% macro oneinch_fusion_transfers_macro(direction) %} -- src/dst


select 
        tx_id
        , block_time
        , block_slot
        , true as fusion -- TODO: make flags
        , taker as resolver -- Check if this is correct logic
        , maker as user -- Check
        , order_hash
        , tx_success
        , call_trace_address
        , token_mint_address
        , symbol 
        , amount 
        , amount_usd 
        , from_owner
        , to_owner
        , array[coalesce(outer_instruction_index, -1), coalesce(inner_instruction_index, -1)] as transfer_trace_address
        -- , CALLTRACEADDRESs

    -- , outer_executing_account
    -- , tx_id
    -- , outer_instruction_index
    -- , block_time
    -- , block_slot
    -- , min_by(token_mint_address, inner_instruction_index) as {{ direction }}_token_mint
    -- , min_by(amount_usd, inner_instruction_index) as {{ direction }}_amount_usd
    -- , max_by(token_mint_address, inner_instruction_index) as dst_token_mint
    -- , max_by(amount_usd, inner_instruction_index) as dst_amount_usd
    -- , coalesce(max(amount_usd), 0) as max_amount_usd
    -- , coalesce(min(amount_usd), 0) as min_amount_usd
from (
    select 
        tx_id
        , block_time
        , block_slot
        , taker
        , maker
        , order_hash
        , tx_success
        , outer_instruction_index
        , call_trace_address
    from {{ ref('oneinch_solana_fusion_calls') }}
    where 
        instruction_type = 'fill'
        {% if is_incremental() %}
            and {{ incremental_predicate('block_time') }}
        {% else %}
            and block_time >= date('{{ oneinch_cfg_macro("project_start_date") }}')
        {% endif %}
) qf
left join (
        select * from {{ ref('tokens_solana_transfers') }}
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