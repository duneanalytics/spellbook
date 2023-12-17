{% macro 
    oneinch_ar_handle_unoswap(
        contract,
        contract_data,
        method,
        method_data,
        blockchain,
        traces_cte,
        pools_list
    )
%}



select
    block_number
    , block_time
    , tx_hash
    , '{{ contract }}' as contract_name
    , '{{ contract_data.version }}' as protocol_version
    , '{{ method }}' as method
    , call_from
    , call_to
    , call_trace_address
    , call_success
    , call_selector
    , coalesce(  
        src_token_address, -- if src_token_address not in params
        if(first_direction = 0, first_token0, first_token1) -- -> if first_direction = 0, then first_token0 is src_token_address, else it's 1 -> first_token1
    ) as src_token_address 
    , if( 
        last_direction is null, -- if 1 or 0 pools in array
        if(first_direction = 0, first_token1, first_token0), -- -> THEN if first_direction = 0, then first_token1 is dst_token_address, else it's 1 -> first_token0
        if(last_direction = 0, last_token1, last_token0) -- -> ELSE if last_direction = 0, then last_token1 is dst_token_address, else it's 1 -> last_token0
    ) as dst_token_address 
    , src_receiver
    , dst_receiver
    , src_amount
    , dst_amount
    , dst_amount_min
    , call_gas_used
    , call_output
    , call_error
    , ordinary
    , pools
    , remains
    , '{{ method_data.router_type }}' as router_type
from (
    select
        call_block_number as block_number
        , call_block_time as block_time
        , call_tx_hash as tx_hash
        , call_from
        , contract_address as call_to
        , call_trace_address
        , call_success
        , call_selector
        , {{ method_data.get("src_token_address", "null") }} as src_token_address
        , {{ method_data.get("dst_token_address", "null") }} as dst_token_address
        , {{ method_data.get("src_receiver", "null") }} as src_receiver
        , {{ method_data.get("dst_receiver", "null") }} as dst_receiver
        , {{ method_data.get("src_amount", "null") }} as src_amount
        , {{ method_data.get("dst_amount", "null") }} as dst_amount
        , {{ method_data.get("dst_amount_min", "null") }} as dst_amount_min
        , call_gas_used
        , call_output
        , call_error
        , if(cardinality(call_pools) > 0, true, false) as ordinary -- if call_pools is not empty
        , if(cardinality(call_pools) > 0
            , try(substr(cast(call_pools[1] as varbinary), 13)) -- get first pool from call_pools
            , substr(call_input, call_input_length - 20 - mod(call_input_length - 4, 32) + 1, 20) -- if pools arr is empty, get pool address from call_input
        ) as first_pool
        , if(cardinality(call_pools) > 1
            , try(substr(cast(call_pools[cardinality(call_pools)] as varbinary), 13)) -- get last pool from call_pools if pools arr length 2+
        ) as last_pool
        , if(cardinality(call_pools) > 0
            , try(bitwise_and( -- binary AND to allocate significant bit: bin byte & bit weight
                bytearray_to_bigint(substr(cast(call_pools[1] as varbinary), {{ method_data.direction_bit }} / 8 + 1, 1)) -- current byte: direction_bit / 8 + 1 -- integer division
                , cast(pow(2, 8 - mod({{ method_data.direction_bit }}, 8)) as bigint) -- 2 ^ (8 - direction_bit % 8) -- bit_weights = array[128, 64, 32, 16, 8, 4, 2, 1]
            )) -- get direction from pools
            , try(bitwise_and( -- binary AND
                bytearray_to_bigint(substr(call_input, call_input_length - mod(call_input_length - 4, 32) - 32 + {{ method_data.direction_bit }} / 8 + 1, 1)) -- current byte: input_length - input_length % 8 - 32 + direction_bit / 8 + 1
                , cast(pow(2, 8 - mod({{ method_data.direction_bit }}, 8)) as bigint) -- 2 ^ (8 - direction_bit % 8) -- bit_weights = array[128, 64, 32, 16, 8, 4, 2, 1]
            )) -- get direction from input
        ) as first_direction
        , if(cardinality(call_pools) > 1
            , try(bitwise_and( -- binary AND to allocate significant bit: bin byte & bit weight
                bytearray_to_bigint(substr(cast(call_pools[cardinality(call_pools)] as varbinary), {{ method_data.direction_bit }} / 8 + 1, 1)) -- current byte: direction_bit / 8 + 1 -- integer division
                , cast(pow(2, 8 - mod({{ method_data.direction_bit }}, 8)) as bigint) -- 2 ^ (8 - direction_bit % 8) -- bit_weights = array[128, 64, 32, 16, 8, 4, 2, 1]
            )) -- get direction from pools
        ) as last_direction
        , if(cardinality(call_pools) > 0
            , transform(call_pools, x -> cast(x as varbinary))
            , array[substr(call_input, call_input_length - 32 - mod(call_input_length - 4, 32) + 1, 32)]
        ) as pools
        , remains
    from (
        select *, {{ method_data["pools"] }} as call_pools
        from {{ source('oneinch_' + blockchain, contract + '_call_' + method) }}
        {% if is_incremental() %}
            where {{ incremental_predicate('call_block_time') }}
        {% endif %}
    )
    join traces_cte using(call_block_number, call_tx_hash, call_trace_address)
)
left join (select pool_address as first_pool, token0 as first_token0, token1 as first_token1 from pools_list) using(first_pool)
left join (select pool_address as last_pool, token0 as last_token0, token1 as last_token1 from pools_list) using(last_pool)



{% endmacro %}