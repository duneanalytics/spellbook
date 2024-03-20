{% macro 
    oneinch_ar_handle_generic(
        contract,
        contract_data,
        method,
        method_data,
        blockchain,
        traces_cte,
        start_date
    )
%}



select
    call_block_number as block_number
    , call_block_time as block_time
    , call_tx_hash as tx_hash
    , '{{ contract }}' as contract_name
    , '{{ contract_data.version }}' as protocol_version
    , '{{ method }}' as method
    , call_from
    , contract_address as call_to
    , call_trace_address
    , call_success
    , call_selector
    , {{ method_data.get("src_token_address", "null") }} as src_token_address
    , {{ method_data.get("dst_token_address", "null") }} as dst_token_address
    , {{ method_data.get("src_receiver", "null") }} as src_receiver
    , {{ method_data.get("dst_receiver", "null") }} as dst_receiver
    , {{ method_data.get("src_token_amount", "null") }} as src_token_amount
    , {{ method_data.get("dst_token_amount", "null") }} as dst_token_amount
    , {{ method_data.get("dst_token_amount_min", "null") }} as dst_token_amount_min
    , call_gas_used
    , call_output
    , call_error
    , call_type
    , null as ordinary
    , null as pools
    , remains
    , '{{ method_data.router_type }}' as router_type
from (
    select *, {{ method_data.get("kit", "null") }} as kit
    from {{ source('oneinch_' + blockchain, contract + '_call_' + method) }}
    {% if is_incremental() %}
        where {{ incremental_predicate('call_block_time') }}
    {% else %}
        where call_block_time >= timestamp '{{ start_date }}'
    {% endif %}
)
join traces_cte using(call_block_number, call_tx_hash, call_trace_address)



{% endmacro %}
