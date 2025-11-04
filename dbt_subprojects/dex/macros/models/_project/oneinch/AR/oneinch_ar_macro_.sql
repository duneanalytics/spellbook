{%- macro
    oneinch_ar_macro_(
        blockchain,
        stream,
        contracts
    )
-%}

{%- set date_from = [blockchain.start, stream.start] | max -%}
{%- set wrapper = blockchain.wrapped_native_token_address -%}
{%- set native = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' -%}


with

raw_calls as (
    select *
        , substr(call_input, call_input_length - mod(call_input_length - 4, 32) + 1) as call_input_remains
    from {{ ref('oneinch_' + blockchain.name + '_ar_raw_calls') }}
    where true
        and block_date >= timestamp '{{ date_from }}' -- it is only needed for simple/easy dates
        {% if is_incremental() %}and {{ incremental_predicate('block_time') }}{% endif %}
)

, decoded as (
    {% for contract, contract_data in contracts.items() %}
        -- CONTRACT: {{ contract }} --
        {% for method, method_data in contract_data.methods.items() if not method_data.get('auxiliary', false) %}
            select
                call_block_date as block_date
                , call_block_number as block_number
                , call_tx_hash as tx_hash
                , call_trace_address
                , {{ method_data.get("src_token_address", "null") }} as src_token_address
                , {{ method_data.get("dst_token_address", "null") }} as dst_token_address
                , {{ method_data.get("src_receiver", "null") }} as src_receiver
                , {{ method_data.get("dst_receiver", "null") }} as dst_receiver
                , {% if method_data["src_token_amount"] == "call_value" %}null{% else %}{{ method_data.get("src_token_amount", "null") }}{% endif %} as src_token_amount
                , {{ method_data.get("dst_token_amount", "null") }} as dst_token_amount
                , {{ method_data.get("dst_token_amount_min", "null") }} as dst_token_amount_min
                , {{ method_data.get("pools", "null") }} as raw_pools
                , {{ method_data.get("pool_type_mask", "null") }} as pool_type_mask
                , {{ method_data.get("pool_type_offset", "null") }} as pool_type_offset
                , {{ method_data.get("direction_mask", "null") }} as direction_mask
                , {{ method_data.get("unwrap_mask", "null") }} as unwrap_mask
                , {{ method_data.get("src_token_mask", "null") }} as src_token_mask
                , {{ method_data.get("src_token_offset", "null") }} as src_token_offset
                , {{ method_data.get("dst_token_mask", "null") }} as dst_token_mask
                , {{ method_data.get("dst_token_offset", "null") }} as dst_token_offset
                , {{ method_data.get("router_type", "null") }} as router_type
                , {% if method_data["src_token_amount"] == "call_value" %}true{% else %}false{% endif %} as src_token_amount_from_value
            from {{ source('oneinch_' + blockchain.name, contract + '_call_' + method) }}
            where true
                and call_block_date >= timestamp '{{ date_from }}' -- it is only needed for simple/easy dates
                {% if is_incremental() %}and {{ incremental_predicate('call_block_time') }}{% endif %}
            {% if not loop.last %}union all{% endif %}
        {% endfor %}
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
)

, pools_list as (
    select
        pool
        , tokens
    from {{ ref('dex_raw_pools') }}
    where true
        and blockchain = '{{ blockchain.name }}'
        and type in ('uniswap_compatible', 'curve_compatible')
    group by 1, 2
)

, processing as (
    select *
        , coalesce(
            src_token_address -- src_token_address from params
            , try(case -- try to get src_token_address from first pool: parsed_pools[1]
                when parsed_pools[1]['pool_type'] = 2 then first_pool_tokens[cast(parsed_pools[1]['src_token_index'] as int) + 1] -- when pool type = 2, i.e Curve pool, than get src token address from first_pool_tokens by src token index
                else first_pool_tokens[cast(parsed_pools[1]['direction'] as int) + 1] -- when other cases, i.e. Uniswap compatible pool, than get src token address from first_pool_tokens by direction
            end)
        ) as pool_src_token_address
        , coalesce(
            dst_token_address -- dst_token_address from params
            , try(case -- try to get dst_token_address from last pool: reverse(parsed_pools)[1]
                when reverse(parsed_pools)[1]['pool_type'] = 2 then last_pool_tokens[cast(reverse(parsed_pools)[1]['dst_token_index'] as int) + 1] -- when pool type = 2, i.e Curve pool, than get dst token address from last_pool_tokens by dst token index
                else last_pool_tokens[cast(bitwise_xor(reverse(parsed_pools)[1]['direction'], 1) as int) + 1] -- when other cases, i.e. Uniswap compatible pool, than get dst token address from last_pool_tokens by direction
            end)
        ) as pool_dst_token_address
        , transform(parsed_pools, x -> map_from_entries(array[
            ('type', substr(cast(x['pool_type'] as varbinary), 32))
            , ('info', substr(cast(x['pool'] as varbinary), 1, 12))
            , ('unwrap', substr(reverse(cast(x['unwrap'] as varbinary)), 1, 1))
            , ('address', substr(cast(x['pool'] as varbinary), 13))
        ])) as pools
    from (
        select *
            , try(substr(cast(call_pools[1] as varbinary), 13)) as first_pool
            , try(substr(cast(reverse(call_pools)[1] as varbinary), 13)) as last_pool
            , transform(call_pools, x -> map_from_entries(array[
                ('pool', x) -- raw pool data in uint256
                , ('pool_type', bitwise_right_shift(bitwise_and(x, pool_type_mask), pool_type_offset))
                , ('direction', bitwise_xor(bit_count(bitwise_and(x, direction_mask), 256), if(protocol_version < 6, 0, 1))) -- until v6, the set direction bit meant the reverse direction, starting from v6, the set direction bit means the ordinary direction
                , ('unwrap', bit_count(bitwise_and(x, unwrap_mask), 256))
                , ('src_token_index', bitwise_right_shift(bitwise_and(x, src_token_mask), src_token_offset))
                , ('dst_token_index', bitwise_right_shift(bitwise_and(x, dst_token_mask), dst_token_offset))
            ])) as parsed_pools
        from (
            select *
                , if(router_type = 'unoswap' and cardinality(raw_pools) = 0
                    , array[bytearray_to_uint256(substr(call_input, call_input_length - 32 - mod(call_input_length - 4, 32) + 1, 32))] -- last 32 bytes of input without remains
                    , raw_pools
                ) as call_pools
                , if(router_type = 'unoswap', cardinality(raw_pools) > 0) as ordinary -- true if call pools is not empty, null for generic
            from decoded
            join raw_calls using(block_date, block_number, tx_hash, call_trace_address)
        )
    )
    left join (select pool as first_pool, tokens as first_pool_tokens from pools_list) using(first_pool)
    left join (select pool as last_pool, tokens as last_pool_tokens from pools_list) using(last_pool)
)

, native_prices as ( -- joining prices at this level, not on "raw_transfers", because there could be a call without transfers for which the tx cost needs to be calculated
    select
        minute
        , price
        , decimals
    from {{ source('prices', 'usd') }}
    where true
        and blockchain = '{{ blockchain.name }}'
        and contract_address = {{ wrapper }}
        and minute >= timestamp '{{ date_from }}'
        {% if is_incremental() %}and {{ incremental_predicate('minute') }}{% endif %}
)

-- output --

select
    blockchain
    , {{ blockchain.chain_id }} as chain_id
    , block_number
    , block_time
    , tx_hash
    , tx_success
    , tx_from
    , tx_to
    , tx_nonce
    , tx_gas_used
    , tx_gas_price
    , tx_priority_fee_per_gas
    , tx_index -- it is necessary to determine the order in the block
    , call_trace_address
    , call_success
    , call_gas_used
    , call_selector
    , call_method
    , call_from
    , call_to
    , call_output
    , call_error
    , call_type
    , protocol
    , protocol_version
    , contract_name
    , src_receiver
    , dst_receiver
    , coalesce(src_token_address, if(element_at(pools[1], 'unwrap') = 0x01 and pool_src_token_address = {{ wrapper }} and call_value > uint256 '0', {{ native }}, pool_src_token_address)) as src_token_address
    , coalesce(dst_token_address, if(element_at(reverse(pools)[1], 'unwrap') = 0x01 and pool_dst_token_address = {{ wrapper }}, {{ native }}, pool_dst_token_address)) as dst_token_address
    , if(src_token_amount_from_value, call_value, src_token_amount) as src_token_amount
    , dst_token_amount
    , dst_token_amount_min
    , router_type
    , pools
    , coalesce(try(transform(sequence(1, length(call_input_remains), 4), x -> bytearray_to_bigint(reverse(substr(reverse(call_input_remains), x, 4))))), array[]) as remains
    , map_from_entries(array[
        ('ordinary', ordinary)
        , ('direct', call_from = tx_from and call_to = tx_to) -- == cardinality(call_trace_address) = 0, but due to zksync trace structure, it is necessary to switch to this
    ]) as flags
    , minute
    , block_date
    , block_month
    , price as native_price
    , decimals as native_decimals
from ({{
    add_tx_columns(
        model_cte = 'processing'
        , blockchain = blockchain.name
        , columns = ['from', 'to', 'nonce', 'gas_price', 'priority_fee_per_gas', 'gas_used', 'index']
    )
}}) as t
left join native_prices using(minute)

{%- endmacro -%}