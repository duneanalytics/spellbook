{% macro 
    oneinch_project_swaps_macro(
        blockchain
        , date_from = '2019-01-01'
    ) 
%}

{% set native_addresses = '(0x0000000000000000000000000000000000000000, 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee)' %}



with

meta as (
    select 
        chain_id
        , wrapped_native_token_address
        , native_token_symbol as native_symbol
    from {{ source('oneinch', 'blockchains') }}
    where blockchain = '{{blockchain}}'
)

, orders as (
    select
        block_number
        , tx_hash
        , call_trace_address
        , project
        , order_hash
        , maker
        , maker_asset
        , making_amount
        , taker_asset
        , taking_amount
        , flags as order_flags
    from {{ ref('oneinch_' + blockchain + '_project_orders') }}
    where
        {% if is_incremental() %}
            {{ incremental_predicate('block_time') }}
        {% else %}
            block_time >= timestamp '{{date_from}}'
        {% endif %}
        and call_success
    
    union all
    
    select
        block_number
        , tx_hash
        , call_trace_address
        , '1inch' as project
        , coalesce(order_hash, concat(tx_hash, to_big_endian_32(cast(counter as int)))) as order_hash
        , maker
        , maker_asset
        , making_amount
        , taker_asset
        , taking_amount
        , flags as order_flags
    from (
        select *, row_number() over(partition by block_number, tx_hash order by call_trace_address) as counter
        from {{ source('oneinch', 'lop') }}
        where
            blockchain = '{{blockchain}}'
            {% if is_incremental() %}
                and {{ incremental_predicate('block_time') }}
            {% else %}
                and block_time >= timestamp '{{date_from}}'
            {% endif %}
            and call_success
    )
)

, calls as (
    select
        blockchain
        , block_number
        , tx_hash
        , tx_from
        , tx_to
        , call_trace_address
        , project
        , tag
        , flags
        , call_selector
        , method
        , call_from
        , call_to
        , order_hash
        , order_flags
        , maker
        , maker_asset
        , making_amount
        , taker_asset
        , taking_amount
        , array_agg(call_trace_address) over(partition by block_number, tx_hash, project) as call_trace_addresses -- to update the array after filtering nested calls of the project
        , if(maker_asset in {{native_addresses}}, wrapped_native_token_address, maker_asset) as _maker_asset
        , if(taker_asset in {{native_addresses}}, wrapped_native_token_address, taker_asset) as _taker_asset
        , coalesce(order_hash, to_big_endian_64(counter)) as call_trade_id -- without call_trade for the correctness of the max transfer approach
    from (
        select
            *
            , array_agg(call_trace_address) over(partition by block_number, tx_hash, project) as call_trace_addresses
            , row_number() over(partition by block_number, tx_hash order by call_trace_address) as counter
        from {{ ref('oneinch_' + blockchain + '_project_calls') }}
        where
            {% if is_incremental() %}
                {{ incremental_predicate('block_time') }}
            {% else %}
                block_time >= timestamp '{{date_from}}'
            {% endif %}
            and (tx_success or tx_success is null)
            and call_success
    )
    left join orders using(block_number, tx_hash, call_trace_address, project)
    join meta on true
    where
        reduce(call_trace_addresses, true, (r, x) -> if(r and x <> call_trace_address and slice(call_trace_address, 1, cardinality(x)) = x, false, r), r -> r) -- only not nested calls of the project in tx
        or order_hash is not null -- all orders
)

, tokens as (
    select
        contract_address
        , symbol
    from {{ source('tokens', 'erc20') }}
    where blockchain = '{{blockchain}}'
)

, prices as (
    select
        contract_address
        , minute
        , price
        , decimals
    from {{ source('prices', 'usd') }}
    where
        {% if is_incremental() %}
            {{ incremental_predicate('minute') }}
        {% else %}
            minute >= timestamp '{{date_from}}'
        {% endif %}
        and blockchain = '{{blockchain}}'
)

, creations as (
    select address, max(block_number) as block_number
    from (
        select address, block_number
        from {{ source(blockchain, 'creation_traces') }}
        
        union all
        
        values
            (0x0000000000000000000000000000000000000000, 0)

        union all
        
        select wrapped_native_token_address, 0
        from meta
    )
    group by 1
)

, swaps as (
    select
        *
        , array_union(senders, receivers) as users
        , array_agg(
            cast(row(
                project
                , call_trace_address
                , tokens
                , if(
                    user_amount_usd is null or caller_amount_usd is null
                    , coalesce(user_amount_usd, caller_amount_usd, call_amount_usd)
                    , greatest(user_amount_usd, caller_amount_usd)
                )
                , order_hash is not null
            ) as row(
                project varchar
                , call_trace_address array(bigint)
                , tokens array(varchar)
                , amount_usd double
                , intent boolean
            ))
        ) over(partition by block_number, tx_hash) as tx_swaps
        , if(
            user_amount_usd is null or caller_amount_usd is null
            , coalesce(user_amount_usd, caller_amount_usd, call_amount_usd)
            , greatest(user_amount_usd, caller_amount_usd)
        ) as amount_usd
    from (
        select
            blockchain
            , calls.block_number
            , calls.tx_hash
            , calls.call_trace_address
            , call_trade_id
            , any_value(block_time) as block_time
            , any_value(tx_from) as tx_from
            , any_value(tx_to) as tx_to
            , any_value(project) as project
            , any_value(tag) as tag
            , any_value(flags) as flags
            , any_value(call_from) as call_from
            , any_value(call_to) as call_to
            , any_value(call_selector) as call_selector
            , any_value(method) as method
            , any_value(order_hash) as order_hash
            , any_value(maker) as maker
            , any_value(maker_asset) as maker_asset
            , any_value(making_amount) as making_amount
            , any_value(taker_asset) as taker_asset
            , any_value(taking_amount) as taking_amount
            , any_value(order_flags) as order_flags
            , array_agg(distinct if(native, native_symbol, symbol)) as tokens
            , max(amount * price / pow(10, decimals)) as call_amount_usd
            , max(amount * price / pow(10, decimals)) filter(where creations_from.block_number is null or creations_to.block_number is null) as user_amount_usd
            , max(amount * price / pow(10, decimals)) filter(where transfer_from = call_from or transfer_to = call_from) as caller_amount_usd
            , array_agg(distinct transfer_from) filter(where creations_from.block_number is null) as senders
            , array_agg(distinct transfer_to) filter(where creations_to.block_number is null) as receivers
        from calls
        join (
            select
                block_number
                , block_time
                , tx_hash
                , transfer_trace_address
                , if(type = 'native', wrapped_native_token_address, contract_address) as contract_address
                , type = 'native' as native
                , amount
                , native_symbol
                , transfer_from
                , transfer_to
                , date_trunc('minute', block_time) as minute
            from (
                select * from {{ source('oneinch', 'parsed_transfers_from_calls') }}
                where blockchain = '{{blockchain}}'
            ), meta
            where
                {% if is_incremental() %}
                    {{ incremental_predicate('block_time') }}
                {% else %}
                    block_time >= timestamp '{{date_from}}'
                {% endif %}
        ) as transfers on
            calls.block_number = transfers.block_number
            and calls.tx_hash = transfers.tx_hash
            and slice(transfer_trace_address, 1, cardinality(call_trace_address)) = call_trace_address -- nested transfers only
            and reduce(array_distinct(call_trace_addresses), call_trace_address, (r, x) -> if(slice(transfer_trace_address, 1, cardinality(x)) = x and x > r, x, r), r -> r) = call_trace_address -- transfers related to the call only
            and (order_hash is null or contract_address in (_maker_asset, _taker_asset) and maker in (transfer_from, transfer_to)) -- transfers related to the order only
        left join prices using(contract_address, minute)
        left join tokens using(contract_address)
        left join creations as creations_from on creations_from.address = transfers.transfer_from
        left join creations as creations_to on creations_to.address = transfers.transfer_to
        group by 1, 2, 3, 4, 5
    )
)

-- output --

select
    blockchain
    , block_number
    , block_time
    , tx_hash
    , tx_from
    , tx_to
    , call_trace_address
    , project
    , tag
    , flags
    , call_selector
    , method
    , call_from
    , call_to
    , order_hash
    , maker
    , maker_asset
    , making_amount
    , taker_asset
    , taking_amount
    , order_flags
    , tokens
    , amount_usd
    , user_amount_usd
    , caller_amount_usd
    , call_amount_usd
    , tx_swaps
    , if(cardinality(users) = 0 or order_hash is null, array_union(users, array[tx_from]), users) as users
    , users as direct_users
    , senders
    , receivers
    , date(date_trunc('month', block_time)) as block_month
    , call_trade_id
from swaps

{% endmacro %}