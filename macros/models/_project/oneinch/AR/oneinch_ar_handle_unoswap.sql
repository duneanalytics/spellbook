{% macro 
    oneinch_ar_handle_unoswap(
        contract,
        contract_data,
        method,
        method_data,
        blockchain,
        traces_cte,
        pools_list,
        native,
        start_date
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
        src_token_address -- src_token_address from params
        , try(case -- try to get src_token_address from first pool: pools[1]
            when pools[1]['pool_type'] = 2 then first_pool_tokens[cast(pools[1]['src_token_index'] as int) + 1] -- when pool type = 2, i.e Curve pool, than get src token address from first_pool_tokens by src token index
            else first_pool_tokens[cast(pools[1]['direction'] as int) + 1] -- when other cases, i.e. Uniswap compatible pool, than get src token address from first_pool_tokens by direction
    end)) as src_token_address
    , coalesce(
        dst_token_address -- dst_token_address from params
        , try(case -- try to get dst_token_address from last pool: reverse(pools)[1]
            when reverse(pools)[1]['unwrap'] = 1 then {{native}} -- when flaf 'unwrap' is set, than set dst token address to native address
            when reverse(pools)[1]['pool_type'] = 2 then last_pool_tokens[cast(reverse(pools)[1]['dst_token_index'] as int) + 1] -- when pool type = 2, i.e Curve pool, than get dst token address from last_pool_tokens by dst token index
            else last_pool_tokens[cast(bitwise_xor(reverse(pools)[1]['direction'], 1) as int) + 1] -- when other cases, i.e. Uniswap compatible pool, than get dst token address from last_pool_tokens by direction
    end)) as dst_token_address
    , src_receiver
    , dst_receiver
    , src_token_amount
    , dst_token_amount
    , dst_token_amount_min
    , call_gas_used
    , call_output
    , call_error
    , call_type
    , ordinary
    , transform(pools, x -> map_from_entries(array[
        ('type', substr(cast(x['pool_type'] as varbinary), 1, 1))
        , ('info', substr(cast(x['pool'] as varbinary), 1, 12))
        , ('address', substr(cast(x['pool'] as varbinary), 13))
    ])) as pools
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
        , {{ method_data.get("src_token_amount", "null") }} as src_token_amount
        , {{ method_data.get("dst_token_amount", "null") }} as dst_token_amount
        , {{ method_data.get("dst_token_amount_min", "null") }} as dst_token_amount_min
        , call_gas_used
        , call_output
        , call_error
        , call_type
        , ordinary
        , substr(cast(call_pools[1] as varbinary), 13) as first_pool
        , substr(cast(reverse(call_pools)[1] as varbinary), 13) as last_pool
        , transform(call_pools, x -> map_from_entries(array[
            ('pool', x) -- raw pool data in uint256
            , ('pool_type', bitwise_right_shift(bitwise_and(x, {{ method_data.get("pool_type_mask", "null") }}), {{ method_data.get("pool_type_offset", "null") }}))
            , ('direction', bitwise_xor(bit_count(bitwise_and(x, {{ method_data.direction_mask }}), 256), if({{ contract_data.version }} < 6, 0, 1))) -- until v6, the set direction bit meant the reverse direction, starting from v6, the set direction bit means the ordinary direction
            , ('unwrap', bit_count(bitwise_and(x, {{ method_data.get("unwrap_mask", "null") }}), 256))
            , ('src_token_index', bitwise_right_shift(bitwise_and(x, {{ method_data.get("src_token_mask", "null") }}), {{ method_data.get("src_token_offset", "null") }}))
            , ('dst_token_index', bitwise_right_shift(bitwise_and(x, {{ method_data.get("dst_token_mask", "null") }}), {{ method_data.get("dst_token_offset", "null") }}))
        ])) as pools
        , remains
    from (
        select
            *
            , if(cardinality({{ method_data["pools"] }}) > 0
                , {{ method_data["pools"] }}
                , array[bytearray_to_uint256(substr(call_input, call_input_length - 32 - mod(call_input_length - 4, 32) + 1, 32))]
            ) as call_pools
            , cardinality({{ method_data["pools"] }}) > 0 as ordinary -- true if call pools is not empty
        from {{ source('oneinch_' + blockchain, contract + '_call_' + method) }}
        join traces_cte using(call_block_number, call_tx_hash, call_trace_address)
        {% if is_incremental() %}
            where {{ incremental_predicate('call_block_time') }}
        {% else %}
            where call_block_time >= timestamp '{{ start_date }}'
        {% endif %}
    )
)
left join (select pool_address as first_pool, tokens as first_pool_tokens from pools_list) using(first_pool)
left join (select pool_address as last_pool, tokens as last_pool_tokens from pools_list) using(last_pool)



{% endmacro %}