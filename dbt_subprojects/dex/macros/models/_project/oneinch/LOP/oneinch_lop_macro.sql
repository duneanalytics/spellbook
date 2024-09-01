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
                , {{ method_data.get("args", "null") }} as args
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
                , substr(input, input_length - mod(input_length - 4, 32) + 1) as remains
                , output as call_output
                , error as call_error
                , call_type
            from {{ ref('oneinch_' + blockchain + '_lop_raw_traces') }}
            where
                {% if is_incremental() %}
                    {{ incremental_predicate('block_time') }}
                {% else %}
                    block_time >= timestamp '{{ contract_data['start'] }}'
                {% endif %}
        ) using(block_number, tx_hash, call_trace_address)
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
)

-- escrow creations --
, creations as (
    select
        block_number as creation_block_number
        , tx_hash as creation_tx_hash
        , trace_address
        , factory
        , escrow
    from {{ ref('oneinch_' + blockchain + '_escrow_creations') }}
    {% if is_incremental() %}where {{ incremental_predicate('block_time') }}{% endif %}
)

-- escrow results --
, results as (
    select
        hashlock
        , escrow
        , token
        , sum(amount) filter(where method = 'cancel') as cancel_amount
        , sum(amount) filter(where method = 'withdraw') as withdraw_amount
        , sum(amount) filter(where method = 'rescueFunds') as rescue_amount
        , max_by(block_time, amount) filter(where method = 'cancel') as main_cancel_time
        , max_by(block_time, amount) filter(where method = 'withdraw') as main_withdraw_time
        , max_by(block_time, amount) filter(where method = 'rescueFunds') as main_rescue_time
        , array_agg(distinct tx_hash) filter(where method = 'cancel') as cancels
        , array_agg(distinct tx_hash) filter(where method = 'withdraw') as withdrawals
        , array_agg(distinct tx_hash) filter(where method = 'rescueFunds') as rescues
    from {{ ref('oneinch_' + blockchain + '_escrow_results') }}
    {% if is_incremental() %}where {{ incremental_predicate('block_time') }}{% endif %}
    -- with an incremental predicate, as the results always come after the creations
    group by 1, 2, 3
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
    , receiver
    , maker_asset
    , making_amount
    , taker_asset
    , taking_amount
    , order_hash
    , map_from_entries(array[
        ('partial', _partial)
        , ('multiple', _multiple)
        , ('fusion', array_position(fusion_settlement_addresses, call_from) > 0 or reduce(fusion_settlement_addresses, false, (r, x) -> r or coalesce(varbinary_position(args, x), 0) > 0, r -> r))
        , ('factoryInArgs', reduce(escrow_factory_addresses, false, (r, x) -> r or coalesce(varbinary_position(args, x), 0) > 0, r -> r))
        , ('first', row_number() over(partition by coalesce(order_hash, tx_hash) order by block_number, tx_index, call_trace_address) = 1)
    ]) as flags
    , concat(
        cast(length(remains) + length(order_remains) as bigint)
        , concat(
            if(length(remains) > 0
                , transform(sequence(1, length(remains), 4), x -> bytearray_to_bigint(reverse(substr(reverse(remains), x, 4))))
                , array[bigint '0']
            )
            , array[bytearray_to_bigint(order_remains)]
        )
    ) as remains
    , if(reduce(escrow_factory_addresses, false, (r, x) -> r or coalesce(varbinary_position(args, x), 0) > 0, r -> r), args, cast(null as varbinary)) as escrow_args
    , factory as escrow_factory
    , hashlock
    , creations.escrow
    , withdraw_amount
    , cancel_amount
    , rescue_amount
    , main_withdraw_time
    , main_cancel_time
    , main_rescue_time
    , withdrawals
    , cancels
    , rescues
    , date_trunc('minute', block_time) as minute
    , date(date_trunc('month', block_time)) as block_month
from ({{
    add_tx_columns(
        model_cte = 'orders'
        , blockchain = blockchain
        , columns = ['from', 'to', 'success', 'nonce', 'gas_price', 'priority_fee_per_gas', 'gas_used', 'index']
    )
}}) as orders
join ({{ oneinch_blockchain_macro(blockchain) }}) on true
left join creations on
    creations.creation_block_number = orders.block_number
    and creations.creation_tx_hash = orders.tx_hash
    and slice(creations.trace_address, 1, cardinality(orders.call_trace_address)) = orders.call_trace_address
left join results on
    results.escrow = creations.escrow
    and results.token = orders.maker_asset

{% endmacro %}