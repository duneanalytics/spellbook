{% macro
    oneinch_lop_macro(
        blockchain
    )
%}



with

orders as (
    {% for contract, contract_data in oneinch_lop_cfg_contracts_macro().items() if blockchain in contract_data['blockchains'] %}
        select * from ({% for method, method_data in contract_data.methods.items() %}
            select
                blockchain
                , call_block_number as block_number
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
                , fusion_settlement_addresses as _settlements
                , reduce(fusion_settlement_addresses, false, (r, x) -> r or coalesce(varbinary_position({{ method_data.get("args", "null")}}, x), 0) > 0, r -> r) as _with_settlement
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
                join ({{ oneinch_blockchain_macro(blockchain) }}) on true
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
        , ('fusion', _with_settlement or array_position(_settlements, call_from) > 0)
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
    , date_trunc('minute', block_time) as minute
    , date(date_trunc('month', block_time)) as block_month

from (
    {{
        add_tx_columns(
            model_cte = 'orders'
            , blockchain = blockchain
            , columns = ['from', 'to', 'success', 'nonce', 'gas_price', 'priority_fee_per_gas', 'gas_used', 'index']
        )
    }}
)

{% endmacro %}
