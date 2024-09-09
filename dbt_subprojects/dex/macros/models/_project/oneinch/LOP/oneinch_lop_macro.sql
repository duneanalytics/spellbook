{% macro oneinch_lop_macro(blockchain) %}



with

orders as (
    {% for contract, contract_data in oneinch_lop_cfg_contracts_macro().items() if blockchain in contract_data['blockchains'] %}
        select * from ({% for method, method_data in contract_data.methods.items() %}
            select
                call_block_number as block_number
                , call_block_time as block_time
                , date(date_trunc('day', call_block_time)) as block_date
                , call_tx_hash as tx_hash
                , '{{ contract }}' as contract_name
                , '{{ contract_data['version'] }}' as protocol_version
                , '{{ method }}' as method
                , call_trace_address
                , contract_address as call_to
                , call_success
                , {{ method_data.get("maker", "null") }} as maker
                , {{ method_data.get("receiver", "null") }} as receiver
                , {{ method_data.get("maker_asset", "null") }} as maker_asset
                , {{ method_data.get("taker_asset", "null") }} as taker_asset
                , {{ method_data.get("making_amount", "null") }} as making_amount
                , {{ method_data.get("taking_amount", "null") }} as taking_amount
                , {{ method_data.get("order_hash", "null") }} as order_hash
                , {{ method_data.get("order_remains", "0x0000000000") }} as order_remains
                , {{ method_data.get("args", "cast(null as varbinary)") }} as args
                , {% if 'partial_bit' in method_data %}
                    try(bitwise_and( -- binary AND to allocate significant bit: necessary byte & mask (i.e. * bit weight)
                        bytearray_to_bigint(substr({{ method_data.maker_traits }}, {{ method_data.partial_bit }} / 8 + 1, 1)) -- current byte: partial_bit / 8 + 1 -- integer division
                        , cast(pow(2, {{ method_data.partial_bit }} - {{ method_data.partial_bit }} / 8 * 8) as bigint) -- 2 ^ (partial_bit - partial_bit / 8 * 8) -- bit_weights = array[128, 64, 32, 16, 8, 4, 2, 1]
                    ) = 0) -- if set, the order does not allow partial fills
                {% else %} null {% endif %} as _partial
                , {% if 'multiple_bit' in method_data %}
                    try(bitwise_and( -- binary AND to allocate significant bit: necessary byte & mask (i.e. * bit weight)
                        bytearray_to_bigint(substr({{ method_data.maker_traits }}, {{ method_data.multiple_bit }} / 8 + 1, 1)) -- current byte: multiple_bit / 8 + 1 -- integer division
                        , cast(pow(2, {{ method_data.multiple_bit }} - {{ method_data.multiple_bit }} / 8 * 8) as bigint) -- 2 ^ (multiple_bit - multiple_bit / 8 * 8) -- bit_weights = array[128, 64, 32, 16, 8, 4, 2, 1]
                    ) > 0) -- if set, the order permits multiple fills
                {% else %} null {% endif %} as _multiple
            from (
                select *, cast(json_parse({{ method_data.get("order", '"order"') }}) as map(varchar, varchar)) as order_map
                from {{ source('oneinch_' + blockchain, contract + '_call_' + method) }}
                {% if is_incremental() %}
                    where {{ incremental_predicate('call_block_time') }}
                {% endif %}
            )
            {% if not loop.last %} union all {% endif %}
        {% endfor %})
        join (
            select
                block_number
                , tx_hash
                , trace_address as call_trace_address
                , "from" as call_from
                , selector as call_selector
                , gas_used as call_gas_used
                , substr(input, input_length - mod(input_length - 4, 32) + 1) as _remains
                , output as call_output
                , error as call_error
                , call_type
            from {{ ref('oneinch_' + blockchain + '_lop_raw_traces') }}
            where
                {% if is_incremental() %}
                    {{ incremental_predicate('block_time') }}
                {% else %}
                    block_time >= greatest(timestamp '{{ contract_data['start'] }}', timestamp {{ oneinch_easy_date() }})
                {% endif %}
        ) using(block_number, tx_hash, call_trace_address)
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
)

-- will be converted to submitted contracts
, SrcEscrowCreated as (
    with
    
    factories as (
        select factory
        from ({{ oneinch_blockchain_macro(blockchain) }}), unnest(escrow_factory_addresses) as f(factory)
    )

    select
        block_number
        , tx_hash
        , contract_address as factory
        , substr(data, 32*0 + 1, 32) as order_hash
        , substr(data, 32*1 + 1, 32) as hashlock
        , substr(data, 32*2 + 12 + 1, 20) as maker
        , substr(data, 32*3 + 12 + 1, 20) as taker
        , substr(data, 32*4 + 12 + 1, 20) as token
        , bytearray_to_uint256(substr(data, 32*5 + 1, 32)) as amount
        , bytearray_to_uint256(substr(data, 32*6 + 1, 32)) as safety_deposit
        , substr(data, 32*7 + 1, 32) as timelocks
    from {{ source(blockchain, 'logs') }}
    where
        contract_address in (select factory from factories)
        and topic0 = 0x0e534c62f0afd2fa0f0fa71198e8aa2d549f24daf2bb47de0d5486c7ce9288ca
        {% if is_incremental() %}
            and {{ incremental_predicate('block_time') }}
        {% else %}
            and block_time > timestamp '2024-08-20'
        {% endif %}
)

, calculations as (
    select
        blockchain
        , orders.*
        , map_from_entries(array[
            ('partial', _partial)
            , ('multiple', _multiple)
            , ('fusion', array_position(fusion_settlement_addresses, call_from) > 0 or reduce(fusion_settlement_addresses, false, (r, x) -> r or coalesce(varbinary_position(args, x), 0) > 0, r -> r))
            , ('factoryInArgs', reduce(escrow_factory_addresses, false, (r, x) -> r or coalesce(varbinary_position(args, x), 0) > 0, r -> r))
        ]) as flags
        , concat(
            cast(length(_remains) + length(order_remains) as bigint)
            , concat(
                if(length(_remains) > 0
                    , transform(sequence(1, length(_remains), 4), x -> bytearray_to_bigint(reverse(substr(reverse(_remains), x, 4))))
                    , array[bigint '0']
                )
                , array[bytearray_to_bigint(order_remains)]
            )
        ) as remains
        , hashlock
        , factory as src_factory
        , if(hashlock is not null, substr(keccak(concat(
            0xff
            , factory
            , keccak(concat(
                orders.order_hash
                , hashlock
                , lpad(SrcEscrowCreated.maker, 32, 0x00)
                , lpad(SrcEscrowCreated.taker, 32, 0x00)
                , lpad(token, 32, 0x00)
                , cast(amount as varbinary)
                , cast(safety_deposit as varbinary)
                , to_big_endian_32(cast(to_unixtime(block_time) as int))
                , substr(timelocks, 5) -- replace the first 4 bytes with current block time
            ))
            , keccak(concat(
                0x3d602d80600a3d3981f3363d3d373d3d3d363d73
                , substr(keccak(concat(0xd6, 0x94, factory, 0x02)), 13) -- src nonce = 2
                , 0x5af43d82803e903d91602b57fd5bf3)
            )
        )), 13)) as src_escrow
        , row_number() over(partition by hashlock order by orders.block_number, orders.tx_hash, call_trace_address) as hashlockNum
    from orders
    join ({{ oneinch_blockchain_macro(blockchain) }}) on true
    left join SrcEscrowCreated on
        SrcEscrowCreated.block_number = orders.block_number
        and SrcEscrowCreated.tx_hash = orders.tx_hash
        and SrcEscrowCreated.order_hash = orders.order_hash
        and varbinary_position(orders.args, SrcEscrowCreated.hashlock) > 0
        and SrcEscrowCreated.maker = orders.maker
        and SrcEscrowCreated.taker = orders.call_from
        and SrcEscrowCreated.token = orders.maker_asset
)

-- createDstEscrow calls on all blockchains --
, dst as (
    select
        blockchain as dst_blockchain
        , block_number as dst_block_number
        , block_time as dst_block_time
        , tx_hash as dst_tx_hash
        , trace_address as dst_trace_address
        , factory as dst_factory
        , escrow as dst_escrow
        , order_hash as dst_order_hash
        , hashlock
        , maker as dst_maker
        , taker as dst_taker
        , token as dst_token
        , amount as dst_amount
        , timelocks
        , call_success as dst_creation_call_success
        , wrapped_native_token_address as dst_wrapper
        , row_number() over(partition by hashlock order by block_number, tx_hash, trace_address) as hashlockNum
    from {{ ref('oneinch_escrow_dst_creations') }}
    join {{ ref('oneinch_blockchains') }} using(blockchain)
    {% if is_incremental() %}
        where {{ incremental_predicate('block_time') }}
    {% endif %}
)

-- output --

select
    blockchain
    , block_number
    , block_time
    , block_date
    , tx_hash
    , tx_from
    , tx_to
    , tx_success
    , tx_nonce
    , tx_gas_used
    , tx_gas_price
    , tx_priority_fee_per_gas
    , contract_name
    , 'LOP' as protocol
    , protocol_version
    , method
    , call_selector
    , call_trace_address
    , call_from
    , call_to
    , call_success
    , call_gas_used
    , call_output
    , call_error
    , call_type
    , maker
    , coalesce(dst_maker, receiver) as receiver
    , maker_asset
    , making_amount
    , coalesce(dst_token, taker_asset) as taker_asset
    , coalesce(dst_amount, taking_amount) as taking_amount
    , order_hash
    , map_concat(flags, map_from_entries(array[
        ('first', row_number() over(partition by coalesce(order_hash, tx_hash) order by block_number, tx_index, call_trace_address) = 1)
    ])) as flags
    , remains
    , src_escrow
    , coalesce(hashlock, cast(null as varbinary)) as hashlock
    , dst_blockchain
    , dst_block_number
    , dst_block_time
    , dst_tx_hash
    , dst_escrow
    , dst_maker
    , dst_taker
    , dst_token
    , dst_amount
    , dst_wrapper
    , dst_creation_call_success
    , args
    , date_trunc('minute', block_time) as minute
    , date(date_trunc('month', block_time)) as block_month
from ({{
    add_tx_columns(
        model_cte = 'calculations'
        , blockchain = blockchain
        , columns = ['from', 'to', 'success', 'nonce', 'gas_price', 'priority_fee_per_gas', 'gas_used', 'index']
    )
}}) as orders
left join dst using(hashlock, hashlockNum)

{% endmacro %}