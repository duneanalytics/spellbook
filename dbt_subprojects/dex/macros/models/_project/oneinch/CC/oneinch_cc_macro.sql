{%- macro
    oneinch_cc_macro(
        blockchain,
        stream,
        contracts,
        initial
    )
-%}

{%- set date_from = [blockchain.start, stream.start] | max -%}
{%- set wrapper = blockchain.wrapped_native_token_address -%}



with

iterations as (
    {% for contract, contract_data in contracts.items() %}
        -- CONTRACT: {{ contract }} --
        {% for method, method_data in contract_data.methods.items() %}
            select
                call_block_number as block_number
                , call_block_date as block_date
                , call_tx_hash as tx_hash
                , call_trace_address
                , {{ method_data.get("flow", "null") }} as flow
                , {{ method_data.get("factory", "null") }} as factory
                , {{ method_data.get("escrow", "null") }} as escrow
                , {{ method_data.get("order_hash", "null") }} as order_hash
                , {{ method_data.get("hashlock", "null") }} as hashlock
                , {{ method_data.get("maker", "null") }} as maker
                , {{ method_data.get("taker", "null") }} as taker
                , {{ method_data.get("token", "null") }} as token
                , {{ method_data.get("amount", "null") }} as amount
                , {{ method_data.get("safety_deposit", "null") }} as safety_deposit
                , {{ method_data.get("timelocks", "null") }} as timelocks
                , {{ method_data.get("secret", "null") }} as secret
                , {{ method_data.get("receiver", "null") }} as receiver
                , {{ method_data.get("nonce", "null") }} as nonce
            from (
                select {%- if contract_data.address == "creations" %} distinct {%- endif %} *
                    , {{ method_data.get("immutables", '"immutables"') }} as data
                from {{ source('oneinch_' + blockchain.name, contract + '_call_' + method) }}
                where true
                    and call_block_date >= timestamp '{{ date_from }}'
                    {% if is_incremental() %}and {{ incremental_predicate('call_block_time') }}{% endif %}
            )
            {% if not loop.last %}union all{% endif %}
        {% endfor %}
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
)

, raw_calls as (
    select *
    from {{ ref('oneinch_' + blockchain.name + '_cc_raw_calls') }}
    where true
        and block_date >= timestamp '{{ date_from }}'
        {% if is_incremental() %}and {{ incremental_predicate('block_time') }}{% endif %}
)

, initial as ( -- required to get the initial order data & initial call data
    select
        iterations.*
        , block_number
        , block_date
        , tx_hash
        , call_trace_address as initial_call_trace_address
        , call_success as initial_call_success
        , call_gas_used as initial_call_gas_used
        , call_selector as initial_call_selector
        , call_method as initial_call_method
        , call_from as initial_call_from
        , call_to as initial_call_to
        , call_output as initial_call_output
        , call_error as initial_call_error
        , call_type as initial_call_type
        , contract_name as initial_contract_name
    from (
        {% for contract, contract_data in initial.items() if 'cc' in contract_data.get('streams', ['lo']) %}
            -- CONTRACT: {{ contract }} --
            {% for method, method_data in contract_data.methods.items() if 'cc' in method_data.get('streams', ['lo']) %}
                select
                    call_block_number as block_number
                    , call_block_date as block_date
                    , call_tx_hash as tx_hash
                    , call_trace_address
                    , {{ method_data.get("maker", "cast(null as varbinary)") }} as order_maker
                    , {{ method_data.get("receiver", "cast(null as varbinary)") }} as order_receiver
                    , {{ method_data.get("maker_asset", "cast(null as varbinary)") }} as order_maker_asset
                    , {{ method_data.get("taker_asset", "cast(null as varbinary)") }} as order_taker_asset
                    , {{ method_data.get("maker_amount", "cast(null as varbinary)") }} as order_maker_amount
                    , {{ method_data.get("taker_amount", "cast(null as varbinary)") }} as order_taker_amount
                    , {{ method_data.get("making_amount", "cast(null as varbinary)") }} as order_making_amount
                    , {{ method_data.get("taking_amount", "cast(null as varbinary)") }} as order_taking_amount
                    , {{ method_data.get("order_hash", "cast(null as varbinary)") }} as order_hash
                    , {{ method_data.get("order_remains", "0x0000000000") }} as order_remains
                from (
                    select *
                        , {{ method_data.get("order", '"order"') }} as data
                        , {{ method_data.get("args", "cast(null as varbinary)") }} as args
                    from {{ source('oneinch_' + blockchain.name, contract + '_call_' + method) }}
                    where true
                        and call_block_date >= timestamp '{{ date_from }}'
                        {% if is_incremental() %}and {{ incremental_predicate('call_block_time') }}{% endif %}
                )
                {% if not loop.last %}union all{% endif %}
            {% endfor %}
            {% if not loop.last %}union all{% endif %}
        {% endfor %}
    ) as iterations
    join (
        select *
        from {{ ref('oneinch_' + blockchain.name + '_lo_raw_calls') }}
        where true
            and block_date >= timestamp '{{ date_from }}'
            {% if is_incremental() %}and {{ incremental_predicate('block_time') }}{% endif %}
    ) as raw_calls using(block_date, block_number, tx_hash, call_trace_address)
)

, native_prices as ( -- joining prices at this level, not on "raw_transfers", because there could be a call without transfers for which the tx cost needs to be calculated
    select
        minute
        , price
        , decimals
    from {{ source('prices', 'usd') }}
    where
        blockchain = '{{ blockchain.name }}'
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
    , tx_index -- it is necessary to determine the order of creations in the block
    , coalesce(initial_call_trace_address, call_trace_address) as call_trace_address -- required for the correct joining of transfers
    , coalesce(initial_call_success, call_success) as call_success
    , coalesce(initial_call_gas_used, call_gas_used) as call_gas_used
    , coalesce(initial_call_selector, call_selector) as call_selector
    , coalesce(initial_call_method, call_method) as call_method
    , coalesce(initial_call_from, call_from) as call_from
    , coalesce(initial_call_to, call_to) as call_to
    , coalesce(initial_call_output, call_output) as call_output
    , coalesce(initial_call_error, call_error) as call_error
    , coalesce(initial_call_type, call_type) as call_type
    , protocol -- from iteration
    , protocol_version -- from iteration
    , coalesce(initial_contract_name, contract_name) as contract_name
    , order_hash
    , hashlock
    , flow
    , coalesce(escrow, substr(keccak(concat(
        0xff
        , factory
        , keccak(concat(
            order_hash
            , hashlock
            , lpad(maker, 32, 0x00)
            , lpad(taker, 32, 0x00)
            , lpad(token, 32, 0x00)
            , cast(amount as varbinary)
            , cast(safety_deposit as varbinary)
            , to_big_endian_32(cast(to_unixtime(block_time) as int))
            , substr(timelocks, 5) -- replace the first 4 bytes with current block time
        ))
        , keccak(concat(
            0x3d602d80600a3d3981f3363d3d373d3d3d363d73
            , substr(keccak(concat(0xd6, 0x94, factory, nonce)), 13) -- src nonce = 2 (0x02), dst nonce = 3 (0x03)
            , 0x5af43d82803e903d91602b57fd5bf3)
        )
    )), 13)) as escrow -- calculations required for the createDstEscrow method
    , maker
    , receiver
    , taker
    , token
    , amount
    , safety_deposit
    , timelocks
    , secret
    , map_from_entries(array[
        ('order_maker', cast(order_maker as varchar))
        , ('order_receiver', cast(order_receiver as varchar))
        , ('order_maker_asset', cast(order_maker_asset as varchar))
        , ('order_taker_asset', cast(order_taker_asset as varchar))
        , ('order_maker_amount', cast(order_maker_amount as varchar))
        , ('order_taker_amount', cast(order_taker_amount as varchar))
        , ('order_making_amount', cast(order_making_amount as varchar))
        , ('order_taking_amount', cast(order_taking_amount as varchar))
    ]) as complement
    , array[bytearray_to_bigint(order_remains)] as remains
    , minute
    , block_date
    , block_month
    , price as native_price
    , decimals as native_decimals
from ({{
    add_tx_columns(
        model_cte = 'iterations'
        , blockchain = blockchain.name
        , columns = ['from', 'to', 'nonce', 'gas_price', 'priority_fee_per_gas', 'gas_used', 'index']
    )
}}) as t
join raw_calls using(block_date, block_number, tx_hash, call_trace_address)
left join initial using(block_date, block_number, tx_hash, order_hash)
left join native_prices using(minute)

{%- endmacro -%}