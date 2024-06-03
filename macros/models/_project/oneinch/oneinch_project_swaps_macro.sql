{% macro 
    oneinch_project_swaps_macro(
        blockchain
        , date_from = '2024-01-01'
    ) 
%}



with

methods as (
    select
        blockchain
        , address as call_to
        , project
        , method
        , selector
        , flags
    from {{ ref('oneinch_' + blockchain + '_mapped_methods') }}
    where flags['swap']
)

, calls as (
    select
        *
        , array_agg((call_trace_address, flags)) over(partition by block_number, tx_hash, project) as call_trace_addresses
    from (
        select
            block_number
            , block_time
            , tx_hash
            , "from" as call_from
            , "to" as call_to
            , trace_address as call_trace_address
            , cardinality(trace_address) = 0 as direct
            , substr(input, 1, 4) as selector
        from {{ source(blockchain, 'traces') }}
        {% if is_incremental() %}
            where {{ incremental_predicate('block_time') }}
        {% else %}
            where block_time >= timestamp '{{date_from}}'
        {% endif %}
            and (tx_success or tx_success is null)
            and success
    )
    join methods using(call_to, selector)
    join (
        select
            block_number
            , hash as tx_hash
            , "from" as tx_from
        from {{ source(blockchain, 'transactions') }}
        {% if is_incremental() %}
            where {{ incremental_predicate('block_time') }}
        {% else %}
            where block_time >= timestamp '{{date_from}}'
        {% endif %}
            and (success or success is null)
            
    ) using(block_number, tx_hash)
)

, prices as (
    select
        blockchain
        , contract_address
        , minute
        , price
        , decimals
        , symbol
    from {{ source('prices', 'usd') }}
    where
        blockchain = '{{blockchain}}'
        {% if is_incremental() %}
            and {{ incremental_predicate('minute') }}
        {% else %}
            and minute >= timestamp '{{date_from}}'
        {% endif %}
)

, creations as (
    select
        address
        , block_number
    from {{ source(blockchain, 'creation_traces') }}
    
    union all
    
    values
        (0x0000000000000000000000000000000000000000, 0)
)

, amounts as (
    select
        *
        , transform(filter(array_distinct(flatten(call_transfer_addresses)), x -> x[2]), x -> (x[1])) as users
        , array_agg(
            cast(row(project, call_trace_address, coalesce(user_amount_usd, amount_usd)) as row(project varchar, call_trace_address array(bigint), amount_usd double))
        ) over(partition by block_number, tx_hash) as amounts
        , coalesce(if(direct, user_amount_usd, caller_amount_usd), amount_usd) as result_amount_usd
    from (
        select
            calls.blockchain
            , calls.block_number
            , calls.tx_hash
            , calls.call_trace_address
            , any_value(calls.block_time) as block_time
            , any_value(calls.tx_from) as tx_from
            , any_value(calls.project) as project
            , any_value(calls.call_from) as call_from
            , any_value(calls.call_to) as call_to
            , any_value(calls.method) as method
            , any_value(calls.flags) as flags
            , any_value(calls.direct) as direct
            , max(amount * price / pow(10, decimals)) as amount_usd
            , max(amount * price / pow(10, decimals)) filter(where creations_from.block_number is null or creations_to.block_number is null) as user_amount_usd
            , max(amount * price / pow(10, decimals)) filter(where transfer_from = call_from or transfer_to = call_from) as caller_amount_usd
            , array_agg(array[
                cast(row(transfer_from, creations_from.block_number is null) as row(address varbinary, success boolean)),
                cast(row(transfer_to, creations_to.block_number is null) as row(address varbinary, success boolean))
            ]) as call_transfer_addresses       
        from calls
        join (
            select *
            from ({{ oneinch_parsed_transfers_from_calls_macro(blockchain) }})
            {% if is_incremental() %}
                where {{ incremental_predicate('block_time') }}
            {% else %}
                where block_time >= timestamp '{{date_from}}'
            {% endif %}
        ) as transfers on
            calls.block_number = transfers.block_number
            and calls.tx_hash = transfers.tx_hash
            and slice(transfer_trace_address, 1, cardinality(call_trace_address)) = call_trace_address
            and reduce(call_trace_addresses, call_trace_address, (r, x) -> if(slice(transfer_trace_address, 1, cardinality(x[1])) = x[1] and r < x[1], x[1], r), r -> r) = call_trace_address
        left join prices on
            prices.contract_address = transfers.contract_address
            and prices.minute = date_trunc('minute', transfers.block_time)
        left join creations as creations_from on creations_from.address = transfers.transfer_from
        left join creations as creations_to on creations_to.address = transfers.transfer_to
        where
            reduce(call_trace_addresses, true, (r, x) -> if(r and x[1] <> call_trace_address and slice(call_trace_address, 1, cardinality(x[1])) = x[1] and x[2] = flags, false, r), r -> r)
        group by 1, 2, 3, 4
    )
)

-- output --

select
    blockchain
    , block_number
    , tx_hash
    , call_trace_address
    , block_time
    , tx_from
    , project
    , call_from
    , call_to
    , method
    , map_concat(flags, map_from_entries(array[('direct', direct)])) as flags
    , amount_usd
    , user_amount_usd
    , caller_amount_usd
    , result_amount_usd
    , amounts
    , if(cardinality(users) = 0 or not flags['limits'], array_union(users, array[tx_from]), users) as users
    , date(date_trunc('month', block_time)) as block_month
from amounts

{% endmacro %}