{% macro oneinch_project_orders_macro(
    blockchain
    , date_from = '2024-11-20'
)%}



{% set wrapping = 'array[0x0000000000000000000000000000000000000000, 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee, wrapped_native_token_address]' %}

with

logs as (
{% for event, event_data in oneinch_project_orders_cfg_events_macro().items() %}
    select
        '{{ blockchain }}' as blockchain
        , '{{event_data["project"]}}' as project
        , block_number
        , block_time
        , tx_hash
        , tx_from
        , tx_to
        , index
        , contract_address
        , topic0
        , '{{ event_data["name"] }}' as event
        , {{ event_data.get("maker", "null") }} as log_maker
        , {{ event_data.get("taker", "null") }} as log_taker
        , {{ event_data.get("receiver", "null") }} as log_receiver
        , {{ event_data.get("pool", "null") }} as log_pool
        , {{ event_data.get("maker_asset", "null") }} as log_maker_asset
        , {{ event_data.get("taker_asset", "null") }} as log_taker_asset
        , try(bytearray_to_uint256({{ event_data.get("maker_max_amount", "null") }})) as log_maker_max_amount
        , try(bytearray_to_uint256({{ event_data.get("taker_max_amount", "null") }})) as log_taker_max_amount
        , try(bytearray_to_uint256({{ event_data.get("maker_min_amount", "null") }})) as log_maker_min_amount
        , try(bytearray_to_uint256({{ event_data.get("taker_min_amount", "null") }})) as log_taker_min_amount
        , try(bytearray_to_uint256({{ event_data.get("making_amount", "null") }})) as log_making_amount
        , try(bytearray_to_uint256({{ event_data.get("taking_amount", "null") }})) as log_taking_amount
        , try(bytearray_to_uint256({{ event_data.get("log_start", "null") }})) as log_start
        , try(bytearray_to_uint256({{ event_data.get("log_end", "null") }})) as log_end
        , try(bytearray_to_uint256({{ event_data.get("log_deadline", "null") }})) as log_deadline
        , try(bytearray_to_uint256({{ event_data.get("maker_fee_amount", "null") }})) as log_maker_fee_amount
        , try(bytearray_to_uint256({{ event_data.get("taker_fee_amount", "null") }})) as log_taker_fee_amount
        , {{ event_data.get("fee_asset", "null") }} as log_fee_asset
        , try(bytearray_to_uint256({{ event_data.get("fee_max_amount", "null") }})) as log_fee_max_amount
        , try(bytearray_to_uint256({{ event_data.get("fee_min_amount", "null") }})) as log_fee_min_amount
        , try(bytearray_to_uint256({{ event_data.get("fee_amount", "null") }})) as log_fee_amount
        , {{ event_data.get("fee_receiver", "null") }} as log_fee_receiver
        , {{ event_data.get("nonce", "null") }} as log_nonce
        , {{ event_data.get("order_hash", "null") }} as log_order_hash
        , topic1
        , topic2
        , topic3
        , data
        , row_number() over(partition by block_number, tx_hash order by index) as log_counter
    from {{ ref('oneinch_' + blockchain + '_project_orders_raw_logs') }}
    where
        topic0 = {{ event }}
        and {% if is_incremental() %}{{ incremental_predicate('block_time') }}
            {% else %}block_time >= timestamp '{{date_from}}'
            {% endif %}
    {% if not loop.last %}union all{% endif %}
{% endfor %}
)

, calls as (
    select *, row_number() over(partition by block_number, tx_hash order by call_trace_address, call_trade) as call_trade_counter -- trade counter in the tx: there may be multiple calls and multiple trades within a call in a single transaction
    from (
        select
            blockchain
            , project
            , tag
            , flags
            , block_number
            , block_time
            , tx_hash
            , tx_success
            , call_from
            , call_to
            , call_trace_address
            , call_success
            , call_selector
            , call_gas_used
            , call_type
            , call_error
            , method
            , auction
            , topic0
            , trade['trade'] as call_trade
            , trade['maker'] as call_maker
            , trade['taker'] as call_taker
            , trade['receiver'] as call_receiver
            , trade['pool'] as call_pool
            , trade['maker_asset'] as call_maker_asset
            , trade['taker_asset'] as call_taker_asset
            , try(bytearray_to_uint256(trade['maker_max_amount'])) as call_maker_max_amount
            , try(bytearray_to_uint256(trade['taker_max_amount'])) as call_taker_max_amount
            , try(bytearray_to_uint256(trade['maker_min_amount'])) as call_maker_min_amount
            , try(bytearray_to_uint256(trade['taker_min_amount'])) as call_taker_min_amount
            , try(bytearray_to_uint256(trade['making_amount']) / bytearray_to_bigint(trade['_taker_parts'])) as call_making_amount -- for multi trades: when exchange many to one or one to many
            , try(bytearray_to_uint256(trade['taking_amount']) / bytearray_to_bigint(trade['_maker_parts'])) as call_taking_amount
            , try(bytearray_to_uint256(substr(trade['start'], 24 + 1, 8))) as call_start
            , try(bytearray_to_uint256(substr(trade['end'], 24 + 1, 8))) as call_end
            , try(bytearray_to_uint256(substr(trade['deadline'], 24 + 1, 8))) as call_deadline
            , try(bytearray_to_uint256(trade['maker_fee_amount'])) as call_maker_fee_amount
            , try(bytearray_to_uint256(trade['taker_fee_amount'])) as call_taker_fee_amount
            , trade['fee_asset'] as call_fee_asset
            , try(bytearray_to_uint256(trade['fee_max_amount'])) as call_fee_max_amount
            , try(bytearray_to_uint256(trade['fee_min_amount'])) as call_fee_min_amount
            , try(bytearray_to_uint256(trade['fee_amount'])) as call_fee_amount
            , trade['fee_receiver'] as call_fee_receiver
            , trade['nonce'] as call_nonce
            , trade['order_hash'] as call_order_hash
            , call_trades -- total trades in the call
            , input
            , output
        from (
        {% for item in oneinch_project_orders_cfg_methods_macro() %}
            select
                blockchain
                , project
                , tag
                , flags
                , block_number
                , block_time
                , tx_hash
                , tx_success
                , "from" as call_from
                , "to" as call_to
                , trace_address as call_trace_address
                , success as call_success
                , substr(input, 1, 4) as call_selector
                , gas_used as call_gas_used
                , call_type
                , error as call_error
                , '{{ item["name"] }}' as method
                , {{ item.get("auction", "false") }} as auction
                , {{ item.get("event", "null") }} as topic0
                , coalesce({{ item.get("number", "1") }}, 1) as call_trades -- total trades in the call
                , transform(sequence(1, coalesce({{ item.get("number", "1") }}, 1)), x -> map_from_entries(array[
                      ('trade',             try(to_big_endian_64(x)))
                    , ('maker',             {{ item.get("maker", "null") }})
                    , ('taker',             {{ item.get("taker", "null") }})
                    , ('receiver',          {{ item.get("receiver", "null") }})
                    , ('pool',              {{ item.get("pool", "null") }})
                    , ('maker_asset',       {{ item.get("maker_asset", "null") }})
                    , ('taker_asset',       {{ item.get("taker_asset", "null") }})
                    , ('maker_max_amount',  {{ item.get("maker_max_amount", "null") }})
                    , ('taker_max_amount',  {{ item.get("taker_max_amount", "null") }})
                    , ('maker_min_amount',  {{ item.get("maker_min_amount", "null") }})
                    , ('taker_min_amount',  {{ item.get("taker_min_amount", "null") }})
                    , ('making_amount',     {{ item.get("making_amount", "null") }})
                    , ('taking_amount',     {{ item.get("taking_amount", "null") }})
                    , ('start',             {{ item.get("start", "null") }})
                    , ('end',               {{ item.get("end", "null") }})
                    , ('deadline',          {{ item.get("deadline", "null") }})
                    , ('fee_asset',         {{ item.get("fee_asset", "null") }})
                    , ('fee_max_amount',    {{ item.get("fee_max_amount", "null") }})
                    , ('fee_min_amount',    {{ item.get("fee_min_amount", "null") }})
                    , ('fee_amount',        {{ item.get("fee_amount", "null") }})
                    , ('fee_receiver',      {{ item.get("fee_receiver", "null") }})
                    , ('nonce',             {{ item.get("nonce", "null") }})
                    , ('order_hash',        {{ item.get("order_hash", "null") }})
                    , ('_maker_parts',      {{ item.get("_maker_parts", "0x01") }})
                    , ('_taker_parts',      {{ item.get("_taker_parts", "0x01") }})
                ])) as trades
                , input
                , output
            from {{ ref('oneinch_' + blockchain + '_project_orders_raw_traces') }}
            join (
                select *, address as "to"
                from {{ ref('oneinch_' + blockchain + '_mapped_contracts') }}
                where
                    blockchain = '{{ blockchain }}'
                    and project = '{{ item["project"] }}'
                    and coalesce({{ item.get("tag", "null") }} = tag, true)
            ) using("to")
            where
                {% if is_incremental() %}{{ incremental_predicate('block_time') }}
                {% else %}block_time > greatest(first_created_at, timestamp '{{date_from}}'){% endif %}
                and substr(input, 1, 4) = {{ item["selector"] }}
                and {{ item.get("condition", "true") }}

            {% if not loop.last %}union all{% endif %}
        {% endfor %}
        ), unnest(trades) as trades(trade)
    )
)

, joined as (
    select
        *
        , coalesce(log_maker, call_maker, call_to) as maker
        , coalesce(log_taker, call_taker) as taker
        , coalesce(log_receiver, call_receiver) as receiver
        , coalesce(log_pool, call_pool) as pool
        , coalesce(log_maker_asset, call_maker_asset) as maker_asset
        , coalesce(log_taker_asset, call_taker_asset) as taker_asset
        , coalesce(log_maker_max_amount, call_maker_max_amount) as maker_max_amount
        , coalesce(log_taker_max_amount, call_taker_max_amount) as taker_max_amount
        , coalesce(log_maker_min_amount, call_maker_min_amount) as maker_min_amount
        , coalesce(log_taker_min_amount, call_taker_min_amount) as taker_min_amount
        , coalesce(log_making_amount, call_making_amount) as making_amount
        , coalesce(log_taking_amount, call_taking_amount) as taking_amount
        , coalesce(log_start, call_start) as order_start
        , coalesce(log_end, call_end) as order_end
        , coalesce(log_deadline, call_deadline) as order_deadline
        , coalesce(log_maker_fee_amount, call_maker_fee_amount) as maker_fee_amount
        , coalesce(log_taker_fee_amount, call_taker_fee_amount) as taker_fee_amount
        , coalesce(log_fee_asset, call_fee_asset) as fee_asset
        , coalesce(log_fee_max_amount, call_fee_max_amount) as fee_max_amount
        , coalesce(log_fee_min_amount, call_fee_min_amount) as fee_min_amount
        , coalesce(log_fee_amount, call_fee_amount) as fee_amount
        , coalesce(log_fee_receiver, call_fee_receiver) as fee_receiver
        , coalesce(log_nonce, call_nonce) as order_nonce
        , coalesce(log_order_hash, call_order_hash, concat(tx_hash, to_big_endian_32(cast(call_trade_counter as int))), concat(tx_hash, to_big_endian_32(cast(index as int)))) as order_hash
        , count(*) over(partition by block_number, tx_hash, call_trace_address, call_trade) as call_trade_logs -- logs for each trade
        , count(*) over(partition by block_number, tx_hash, index) as log_call_trades -- trades for each log
    from calls
    full join logs using(blockchain, block_number, block_time, tx_hash, topic0, project)
    join (
        select * from {{ source('oneinch', 'blockchains') }}
        where blockchain = '{{blockchain}}'
    ) using(blockchain)
    where
            coalesce(call_maker = log_maker, true)
        and coalesce(call_taker = log_taker, true)
        and coalesce(call_receiver = log_receiver, true)
        and coalesce(call_pool = log_pool, true)
        and (coalesce(call_maker_asset = log_maker_asset, true) or cardinality(array_intersect(array[0x0000000000000000000000000000000000000000, 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee, wrapped_native_token_address], array[call_maker_asset, log_maker_asset])) = 2)
        and (coalesce(call_taker_asset = log_taker_asset, true) or cardinality(array_intersect(array[0x0000000000000000000000000000000000000000, 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee, wrapped_native_token_address], array[call_taker_asset, log_taker_asset])) = 2)
        and coalesce(call_maker_max_amount = log_maker_max_amount, true)
        and coalesce(call_taker_max_amount = log_taker_max_amount, true)
        and coalesce(call_maker_min_amount = log_maker_min_amount, true)
        and coalesce(call_taker_min_amount = log_taker_min_amount, true)
        and coalesce(call_making_amount = log_making_amount or coalesce(if(log_fee_amount < log_making_amount, call_making_amount = log_making_amount - log_fee_amount), false), true)
        and coalesce(call_taking_amount = log_taking_amount or coalesce(if(log_fee_amount < log_taking_amount, call_taking_amount = log_taking_amount - log_fee_amount), false), true)
        and coalesce(call_start = log_start, true)
        and coalesce(call_end = log_end, true)
        and coalesce(call_deadline = log_deadline, true)
        and coalesce(call_nonce = log_nonce, true)
        and coalesce(call_order_hash = log_order_hash, true)
)

-- output --

select
    blockchain
    , project
    , tag
    , map_concat(flags, map_from_entries(array[
        ('auction', coalesce(auction, false) and coalesce(try(order_end - order_start > uint256 '0'), true))
    ])) as flags
    , block_number
    , block_time
    , tx_hash
    , tx_from
    , tx_to
    , tx_success
    , call_from
    , call_to
    , call_trace_address
    , call_success
    , call_selector
    , call_gas_used
    , call_type
    , call_error
    , coalesce(bytearray_to_bigint(call_trade), 1) as call_trade
    , call_trades
    , method
    , maker
    , taker
    , receiver
    , pool
    , maker_asset
    , taker_asset
    , maker_max_amount
    , taker_max_amount
    , maker_min_amount
    , taker_min_amount
    , coalesce(making_amount, try(if(order_start = uint256 '0' or order_start = order_end, maker_max_amount, maker_max_amount - cast(to_unixtime(block_time) - order_start as double) / (order_end - order_start) * (cast(maker_max_amount as double) - cast(maker_min_amount as double)))), maker_max_amount, maker_min_amount) as making_amount
    , coalesce(taking_amount, try(if(order_start = uint256 '0' or order_start = order_end, taker_max_amount, taker_max_amount - cast(to_unixtime(block_time) - order_start as double) / (order_end - order_start) * (cast(taker_max_amount as double) - cast(taker_min_amount as double)))), taker_max_amount, taker_min_amount) as taking_amount
    , order_start
    , order_end
    , order_deadline
    , maker_fee_amount
    , taker_fee_amount
    , fee_asset
    , fee_max_amount
    , fee_min_amount
    , fee_amount
    , fee_receiver
    , order_nonce
    , order_hash
    , array[input] as call_input
    , array[output] as call_output
    , index as event_index
    , contract_address as event_contract_address
    , topic0 as event_topic0
    , topic1 as event_topic1
    , topic2 as event_topic2
    , topic3 as event_topic3
    , array[data] as event_data
    , to_unixtime(block_time) as block_unixtime
    , date(date_trunc('month', block_time)) as block_month
from joined
where
    call_trade_logs = 1 -- if the log was not found or if found the only one log for a trade in the call
    or log_call_trades = 1 -- if the call was not found or if found the only one call trade for the log
    or call_trade_counter = log_counter -- if found several logs

{% endmacro %}